package config

import (
	"encoding/json"
	"github.com/nobelchowdary/agent-os/internal/model"
	"math"
	"strings"
	"testing"
	"time"
)

func TestExecutionOperationsSettingsAreBoundedAndDefaulted(t *testing.T) {
	c, err := Load("../../config/example.json")
	if err != nil {
		t.Fatal(err)
	}
	c.StartupTimeoutSeconds, c.ResultRetentionSeconds, c.KubernetesQPS, c.KubernetesBurst = 0, 0, 0, 0
	qps, burst := c.ClientLimits()
	if err := c.Validate(); err != nil || c.StartupTimeout() != 5*time.Minute || c.ResultRetention() != 24*time.Hour || qps != 50 || burst != 100 {
		t.Fatal("legacy operational defaults changed", err)
	}
	c.StartupTimeoutSeconds, c.ResultRetentionSeconds, c.KubernetesQPS, c.KubernetesBurst = 2, 3, 70, 120
	qps, burst = c.ClientLimits()
	if err := c.Validate(); err != nil || c.StartupTimeout() != 2*time.Second || c.ResultRetention() != 3*time.Second || qps != 70 || burst != 120 {
		t.Fatal("administrator limits ignored", err)
	}
	for _, mutate := range []func(*Config){
		func(c *Config) { c.StartupTimeoutSeconds = -1 }, func(c *Config) { c.StartupTimeoutSeconds = 86401 },
		func(c *Config) { c.ResultRetentionSeconds = -1 }, func(c *Config) { c.ResultRetentionSeconds = 30*86400 + 1 },
		func(c *Config) { c.KubernetesQPS = -1 }, func(c *Config) { c.KubernetesQPS = .5 }, func(c *Config) { c.KubernetesQPS = 1001 },
		func(c *Config) { c.KubernetesQPS = float32(math.NaN()) }, func(c *Config) { c.KubernetesQPS = float32(math.Inf(1)) },
		func(c *Config) { c.KubernetesBurst = -1 }, func(c *Config) { c.KubernetesBurst = 2001 },
	} {
		bad := c
		mutate(&bad)
		if bad.Validate() == nil {
			t.Fatal("accepted invalid operational bound")
		}
	}
	before, err := c.Resolve(model.CreateRequest{Template: "python"})
	if err != nil {
		t.Fatal(err)
	}
	c.StartupTimeoutSeconds, c.ResultRetentionSeconds, c.KubernetesQPS, c.KubernetesBurst = 10, 20, 100, 200
	after, err := c.Resolve(model.CreateRequest{Template: "python"})
	if err != nil {
		t.Fatal(err)
	}
	one, _ := json.Marshal(before)
	two, _ := json.Marshal(after)
	if string(one) != string(two) {
		t.Fatal("operator tuning changed resolved specification/idempotency fingerprint")
	}
}

func TestExplicitRuntimeProfilesAndLegacyCompatibility(t *testing.T) {
	legacy, err := Load("../../config/example.json")
	if err != nil {
		t.Fatal(err)
	}
	s, err := legacy.Resolve(model.CreateRequest{Template: "python"})
	if err != nil || s.RuntimeProfile != nil || s.RuntimeClass != "gvisor" {
		t.Fatal("legacy gVisor representation changed", s, err)
	}
	encoded, _ := json.Marshal(s)
	if strings.Contains(string(encoded), "runtime_profile") {
		t.Fatal("legacy fingerprint representation changed")
	}
	for _, file := range []string{"gvisor.example.json", "nitro-isolated.example.json"} {
		c, err := Load("../../config/" + file)
		if err != nil {
			t.Fatal(file, err)
		}
		s, err := c.Resolve(model.CreateRequest{Template: "python"})
		if err != nil || s.RuntimeProfile == nil || *s.RuntimeProfile != *c.RuntimeProfile || s.RuntimeClass != c.RuntimeProfile.RuntimeClass {
			t.Fatal(file, s, err)
		}
		s.RuntimeProfile.RequiredHandler = "changed"
		if c.RuntimeProfile.RequiredHandler == "changed" {
			t.Fatal("resolved profile aliases administrator configuration")
		}
	}
}

func TestRuntimeProfileRejectsMissingConflictingOrWeakenedSelection(t *testing.T) {
	valid, err := Load("../../config/example.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []model.RuntimeProfile{
		{},
		{Kind: "gvisor", RuntimeClass: "gvisor"},
		{Kind: "gvisor", RuntimeClass: "gvisor", RequiredHandler: "runc"},
		{Kind: "gvisor", RuntimeClass: "default", RequiredHandler: "runsc"},
		{Kind: "gvisor", RuntimeClass: "isolated-sandbox", RequiredHandler: "runsc"},
		{Kind: "aws-nitro-isolated", RuntimeClass: "isolated-sandbox", RequiredHandler: "runsc"},
		{Kind: "aws-nitro-isolated", RuntimeClass: "custom", RequiredHandler: "isolated-sandbox"},
		{Kind: "aws-nitro-confidential", RuntimeClass: "confidential-sandbox", RequiredHandler: "confidential-sandbox"},
	} {
		c := valid
		c.RuntimeClass, c.RuntimeProfile = "", &p
		if c.Validate() == nil {
			t.Errorf("accepted runtime profile %+v", p)
		}
		if _, err := c.Resolve(model.CreateRequest{Template: "python"}); err == nil {
			t.Errorf("resolved invalid profile %+v", p)
		}
	}
	c := valid
	c.RuntimeClass = ""
	if c.Validate() == nil {
		t.Fatal("accepted missing runtime selection")
	}
	c = valid
	c.RuntimeProfile = &model.RuntimeProfile{Kind: "gvisor", RuntimeClass: "gvisor", RequiredHandler: "runsc"}
	if c.Validate() == nil {
		t.Fatal("accepted ambiguous legacy and explicit profiles")
	}
}

func TestNitroScratchMustLeaveMemoryForExecution(t *testing.T) {
	c, err := Load("../../config/nitro-isolated.example.json")
	if err != nil {
		t.Fatal(err)
	}
	p := c.Templates["python"]
	p.Resources.DiskMiB = p.Resources.MemoryMiB
	c.Templates["python"] = p
	if c.Validate() == nil {
		t.Fatal("accepted scratch at or above the memory limit")
	}
}

func TestEnvironmentPolicy(t *testing.T) {
	c, err := Load("../../config/example.json")
	if err != nil {
		t.Fatal(err)
	}
	s, err := c.Resolve(model.CreateRequest{Template: "python"})
	if err != nil || s.TimeoutSeconds != 60 || s.Resources.CPUMilli != 500 {
		t.Fatal(s, err)
	}
	s.Command[0] = "changed"
	if c.Templates["python"].Command[0] != "python" {
		t.Fatal("resolved command aliases template")
	}
	if _, err = c.Resolve(model.CreateRequest{Template: "python", TimeoutSeconds: 301}); err == nil {
		t.Fatal("timeout policy bypass")
	}
	p := c.Templates["python"]
	p.Image = "python:latest"
	c.Templates["python"] = p
	if c.Validate() == nil {
		t.Fatal("mutable image accepted")
	}
}

func TestTerminationGraceBelongsToTemplateAndPreservesLegacySpec(t *testing.T) {
	c, err := Load("../../config/example.json")
	if err != nil {
		t.Fatal(err)
	}
	template := c.Templates["python"]
	for _, seconds := range []int64{-1, 31} {
		template.TerminationGracePeriodSeconds = seconds
		c.Templates["python"] = template
		if c.Validate() == nil {
			t.Fatal("invalid grace accepted", seconds)
		}
	}
	for _, seconds := range []int64{0, 1, 10, 30} {
		template.TerminationGracePeriodSeconds = seconds
		c.Templates["python"] = template
		if err := c.Validate(); err != nil {
			t.Fatal(err)
		}
		spec, err := c.Resolve(model.CreateRequest{Template: "python"})
		if err != nil || spec.TerminationGracePeriodSeconds != seconds {
			t.Fatal("template grace not resolved", spec, err)
		}
		encoded, err := json.Marshal(spec)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(encoded), "termination_grace_period_seconds") != (seconds != 0) {
			t.Fatal("legacy grace omission or explicit policy lost", string(encoded))
		}
	}
}

func TestCredentialConfiguration(t *testing.T) {
	c, err := Load("../../config/example.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Principals(func(string) string { return "short" }); err == nil {
		t.Fatal("short credential accepted")
	}
	c.Tenants = append(c.Tenants, Tenant{ID: "other", TokenEnv: "OTHER_TOKEN"})
	if _, err := c.Principals(func(string) string { return strings.Repeat("a", 32) }); err == nil {
		t.Fatal("shared credential accepted")
	}
	if _, err := c.Principals(func(s string) string {
		if s == "OTHER_TOKEN" {
			return strings.Repeat("b", 32)
		}
		return strings.Repeat("a", 32)
	}); err != nil {
		t.Fatal(err)
	}
}
