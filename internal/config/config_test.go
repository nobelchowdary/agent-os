package config

import (
	"github.com/nobelchowdary/agent-os/internal/model"
	"strings"
	"testing"
)

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
