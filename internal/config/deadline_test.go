package config

import (
	"testing"

	"github.com/nobelchowdary/agent-os/internal/model"
)

func TestNewAdmissionsPersistAdministratorDeadlineScope(t *testing.T) {
	c, err := Load("../../config/example.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, scope := range []model.DeadlineScope{"", model.DeadlinePod, model.DeadlineJob} {
		template := c.Templates["python"]
		template.DeadlineScope = scope
		c.Templates["python"] = template
		if err := c.Validate(); err != nil {
			t.Fatal(err)
		}
		got, err := c.Resolve(model.CreateRequest{Template: "python"})
		want := scope
		if want == "" {
			want = model.DeadlinePod
		}
		if err != nil || got.DeadlineScope != want {
			t.Fatalf("configured %q resolved %q, %v; want %q", scope, got.DeadlineScope, err, want)
		}
	}
	bad := c.Templates["python"]
	bad.DeadlineScope = "process"
	c.Templates["python"] = bad
	if c.Validate() == nil {
		t.Fatal("accepted an unsupported clock")
	}
	if _, err := c.Resolve(model.CreateRequest{Template: "python"}); err == nil {
		t.Fatal("resolved an unsupported clock")
	}
}
