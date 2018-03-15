package utils

import (
	"os"

	cli "github.com/jawher/mow.cli"
	logging "github.com/op/go-logging"
)

var (
	formatter = logging.MustStringFormatter(
		`%{color}%{time:15:04:05.000} %{shortpkg}.%{shortfunc} [%{level:.4s}]%{color:reset} %{message}`)
	formatterNoColor = logging.MustStringFormatter(
		`%{time:15:04:05.000} %{shortpkg}.%{shortfunc} [%{level:.4s}] %{message}`)
)

func SetupLogging(app *cli.Cli, logger *logging.Logger) {
	logNoColor := app.Bool(cli.BoolOpt{
		Name:   "log-no-color",
		Value:  false,
		Desc:   "disable logging colors",
		EnvVar: "GREMLIN_LOG_NO_COLOR",
	})
	logLevel := app.String(cli.StringOpt{
		Name:   "log-level",
		Value:  "DEBUG",
		Desc:   "logging level",
		EnvVar: "GREMLIN_LOG_LEVEL",
	})
	app.Before = func() {
		stdBackend := logging.NewLogBackend(os.Stderr, "", 0)
		logging.SetBackend(stdBackend)

		if *logNoColor {
			logging.SetFormatter(formatterNoColor)
		} else {
			logging.SetFormatter(formatter)
		}
		level, err := logging.LogLevel(*logLevel)
		if err != nil {
			logger.Errorf("Failed setting log level %s, using ERROR", *logLevel)
		}
		logging.SetLevel(level, logger.Module)
	}
}
