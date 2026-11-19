package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
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
	configPath := flag.String("config", "config/example.json", "administrator-owned configuration")
	dbPath := flag.String("db", "data/runs.db", "durable database in a private directory")
	listen := flag.String("listen", "127.0.0.1:8080", "HTTP listen address; use a TLS proxy for remote clients")
	kubeconfig := flag.String("kubeconfig", "", "kubeconfig path (default in-cluster, then standard kubeconfig)")
	preview := flag.Bool("preview", false, "render environment plans without cluster access; execution disabled")
	flag.Parse()
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
		client, err = kubernetes.NewForConfig(rc)
		if err != nil {
			return err
		}
	}
	b, err := kubebackend.New(client, cfg.Namespace, cfg.RuntimeClass)
	if err != nil {
		return err
	}
	s := &api.Server{Config: cfg, Principals: principals, Store: db, Backend: b, Preview: *preview, Render: func(r model.Record) (any, error) { return b.Render(r) }}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan struct{})
	if !*preview {
		go func() {
			defer close(done)
			(&controller.Controller{Store: db, Backend: b, Logger: slog.Default()}).Run(ctx)
		}()
	} else {
		close(done)
	}
	h := &http.Server{Addr: *listen, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 * 1024}
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
	_ = h.Shutdown(shutdown)
	<-done
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
