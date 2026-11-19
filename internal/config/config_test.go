package config

import (
	"encoding/json"
	"github.com/nobelchowdary/agent-os/internal/model"
	"strings"
	"testing"
)

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
