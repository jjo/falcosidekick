package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type mockOS struct {
	envMap map[string]string
}

func newMockOS() *mockOS {
	return &mockOS{
		envMap: make(map[string]string),
	}
}

func (m *mockOS) Getenv(key string) string {
	if v, ok := m.envMap[key]; ok {
		return v
	}
	return ""
}

func (m *mockOS) Setenv(key, value string) error {
	m.envMap[key] = value
	return nil
}

func Test_altEnvs(t *testing.T) {
	cases := []struct {
		name string
		env  []string
		args []string
		want string
	}{
		{
			name: "Test no altEnvs match",
			env:  []string{"FOO=BAR"},
			args: []string{
				"FOOX",
				"FOOY",
			},
			want: "",
		},
		{
			name: "Test simple altEnvs match",
			env:  []string{"FOO=BAR"},
			args: []string{
				"FOOX",
				"FOO",
			},
			want: "BAR",
		},
		{
			name: "Test +postfix altEnvs match",
			env:  []string{"FOO=BAR"},
			args: []string{
				"FOOX",
				"FOO+/qqq/xyz",
			},
			want: "BAR/qqq/xyz",
		},
		{
			name: "Test 1st match wins",
			env:  []string{"FOO=BAR", "QQQ=XYZ"},
			args: []string{
				"QQQ",
				"FOO",
			},
			want: "XYZ",
		},
	}
	for _, c := range cases {
		defOS = newMockOS()
		for _, e := range c.env {
			s := strings.Split(e, "=")
			defOS.Setenv(s[0], s[1])
		}
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, altEnvs(c.args))
		})
	}
}
