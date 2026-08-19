// Package main provides the Backpack command-line interface workflow.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/k4k3ru-hub/backpack/go/rest"
	k4k3ruCLI "github.com/k4k3ru-hub/cli/go"
)

// Option contains injectable CLI dependencies.
type Option struct {
	HTTPClient rest.HTTPClient
}

// Run executes one Backpack CLI command.
//
// Supported command:
//   - rest markets
//
// Parameters:
//   - ctx: command context
//   - args: arguments following the executable name
//   - stdout: successful JSON output destination
//   - stderr: flag usage and parsing error destination
//   - option: injectable command dependencies
//
// Version:
//   - 2026-08-20: Added.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer, option *Option) error {
	if ctx == nil {
		return fmt.Errorf("failed to run cli: context=null")
	}
	if stdout == nil {
		return fmt.Errorf("failed to run cli: stdout=null")
	}
	if stderr == nil {
		return fmt.Errorf("failed to run cli: stderr=null")
	}
	application, err := newApplication(ctx, option)
	if err != nil {
		return fmt.Errorf("failed to run cli: %w", err)
	}
	if err := application.SetIO(strings.NewReader(""), stdout, stderr); err != nil {
		return fmt.Errorf("failed to run cli: %w", err)
	}
	if err := application.RunArgs(args); err != nil {
		return fmt.Errorf("failed to run cli: %w", err)
	}
	return nil
}

func newApplication(ctx context.Context, option *Option) (*k4k3ruCLI.CLI, error) {
	application := k4k3ruCLI.NewCLIWithName("backpack", nil)
	restCommand := k4k3ruCLI.NewCommand("rest")
	restCommand.SetUsage("Execute a Backpack public REST operation.")
	marketsCommand := k4k3ruCLI.NewCommand("markets")
	marketsCommand.SetUsage("Get Backpack market metadata.")
	if err := marketsCommand.SetArgumentCount(0, 0); err != nil {
		return nil, fmt.Errorf("failed to create markets command: %w", err)
	}
	definitions := []struct {
		name   string
		option k4k3ruCLI.Option
	}{
		{name: "market-types", option: k4k3ruCLI.Option{Description: "Comma-separated market types: SPOT, PERP, IPERP, DATED, PREDICTION, RFQ"}},
		{name: "base-url", option: k4k3ruCLI.Option{DefaultValue: rest.DefaultBaseURL, Description: "Backpack REST base URL"}},
	}
	for _, definition := range definitions {
		if err := marketsCommand.AddOption(definition.name, definition.option); err != nil {
			return nil, fmt.Errorf("failed to create markets command: %w: option_name=%q", err, definition.name)
		}
	}
	marketsCommand.SetAction(func(commandContext *k4k3ruCLI.Context) error {
		return runMarkets(ctx, commandContext, option)
	})
	if err := restCommand.AddCommand(marketsCommand); err != nil {
		return nil, fmt.Errorf("failed to create rest command: %w", err)
	}
	if err := application.Root().AddCommand(restCommand); err != nil {
		return nil, fmt.Errorf("failed to create cli application: %w", err)
	}
	return application, nil
}

func runMarkets(ctx context.Context, commandContext *k4k3ruCLI.Context, option *Option) error {
	if commandContext == nil {
		return fmt.Errorf("failed to run markets command: command_context=null")
	}
	value := func(name string) string {
		parsed, ok := commandContext.Option(name)
		if !ok {
			return ""
		}
		return parsed.Value
	}
	marketTypes, err := parseMarketTypes(value("market-types"))
	if err != nil {
		return fmt.Errorf("failed to run markets command: %w", err)
	}
	clientOptions := []rest.ClientOption{rest.WithBaseURL(value("base-url"))}
	if option != nil && option.HTTPClient != nil {
		clientOptions = append(clientOptions, rest.WithHTTPClient(option.HTTPClient))
	}
	client, err := rest.NewClient(clientOptions...)
	if err != nil {
		return fmt.Errorf("failed to run markets command: %w", err)
	}
	result, err := client.Markets().GetMarkets(ctx, rest.MarketsParams{MarketTypes: marketTypes})
	if err != nil {
		return fmt.Errorf("failed to run markets command: %w", err)
	}
	encoder := json.NewEncoder(commandContext.Output())
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		return fmt.Errorf("failed to run markets command: failed to encode result: %w", err)
	}
	return nil
}

func parseMarketTypes(value string) ([]rest.MarketType, error) {
	if value == "" {
		return nil, nil
	}
	parts := strings.Split(value, ",")
	marketTypes := make([]rest.MarketType, 0, len(parts))
	seen := make(map[rest.MarketType]struct{}, len(parts))
	for _, part := range parts {
		marketType := rest.MarketType(strings.TrimSpace(part))
		if marketType == "" {
			return nil, fmt.Errorf("failed to parse market types option: market_types=invalid")
		}
		if _, exists := seen[marketType]; exists {
			continue
		}
		seen[marketType] = struct{}{}
		marketTypes = append(marketTypes, marketType)
	}
	return marketTypes, nil
}
