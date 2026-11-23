package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nobelchowdary/agent-os/internal/api"
	kubebackend "github.com/nobelchowdary/agent-os/internal/backend/kubernetes"
	"github.com/nobelchowdary/agent-os/internal/config"
	"github.com/nobelchowdary/agent-os/internal/controller"
	"github.com/nobelchowdary/agent-os/internal/model"
	"github.com/nobelchowdary/agent-os/internal/store"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	if err := run(); err != nil {
		slog.Error("agentos stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	// Filesystem-authorized maintenance needs neither API credentials nor a
	// Kubernetes connection and must never start the execution controller.
	if len(os.Args) > 1 && os.Args[1] == "store" {
		return runStore(os.Args[2:], os.Stdout)
	}
	configPath := flag.String("config", "config/example.json", "administrator-owned configuration")
	dbPath := flag.String("db", "data/runs.db", "durable database in a private directory")
	listen := flag.String("listen", "127.0.0.1:8080", "HTTP listen address; use a TLS proxy for remote clients")
	kubeconfig := flag.String("kubeconfig", "", "kubeconfig path (default in-cluster, then standard kubeconfig)")
	preview := flag.Bool("preview", false, "render environment plans without cluster access; execution disabled")
	flag.Parse()
	if flag.NArg() != 0 {
		return errors.New("unexpected arguments; use agentos store --help for database maintenance")
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	principals, err := cfg.Principals(os.Getenv)
	if err != nil {
		return err
	}
	db, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	var client kubernetes.Interface
	if !*preview {
		var rc *rest.Config
		if *kubeconfig != "" {
			rc, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
		} else {
			rc, err = rest.InClusterConfig()
			if err != nil {
				rc, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{}).ClientConfig()
			}
		}
		if err != nil {
			return err
		}
		rc.Timeout = 5 * time.Second
		// Explicit shared clientset limits avoid client-go's small per-group
		// defaults while retaining a bounded administrator-selected request rate.
		rc.QPS, rc.Burst = cfg.ClientLimits()
		client, err = kubernetes.NewForConfig(rc)
		if err != nil {
			return err
		}
	}
	profile, err := cfg.ResolvedRuntimeProfile()
	if err != nil {
		return err
	}
	b, err := kubebackend.NewWithProfile(client, cfg.Namespace, profile)
	if err != nil {
		return err
	}
	s := &api.Server{Config: cfg, Principals: principals, Store: db, Backend: b, Preview: *preview, Render: func(r model.Record) (any, error) { return b.Render(r) }}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan struct{})
	if !*preview {
		c := &controller.Controller{Store: db, Backend: b, Logger: slog.Default(), StartupTimeout: cfg.StartupTimeout(), ResultRetention: cfg.ResultRetention()}
		s.ControllerStatus = c.Status
		go func() {
			defer close(done)
			c.Run(ctx)
		}()
	} else {
		close(done)
	}
	h := &http.Server{Addr: *listen, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 * 1024}
	// Live streams and backend requests must leave before the database closes.
	// Normal requests retain finite timeouts; SSE adds a deadline per flush.
	h.BaseContext = func(net.Listener) context.Context { return ctx }
	errCh := make(chan error, 1)
	go func() {
		slog.Info("agentos listening", "address", *listen, "preview", *preview)
		errCh <- h.ListenAndServe()
	}()
	select {
	case err = <-errCh:
		stop()
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.Shutdown(shutdown); err != nil {
		_ = h.Close()
	}
	<-done
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
