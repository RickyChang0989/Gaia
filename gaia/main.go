package main

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/aerogo/aero"
	"github.com/sirupsen/logrus"
	"newtype.games/gaia/worker/pool"
)

func getPort() int {

	if os.Getenv("PORT") == "" {
		return 8080
	}

	port, err := strconv.Atoi(os.Getenv("PORT"))

	if err != nil {
		return 8080
	}

	return port
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	workerPool, err := pool.New(ctx, 10, 10)

	if err != nil {
		logrus.Fatalf("initial pool failed, %v", err)
		return
	}

	app := aero.New()

	app.Get("/createJob/:id", func(ctx aero.Context) error {
		logrus.Infof("receive create job request: %s", ctx.Get("id"))
		jobId := workerPool.Push(func() {
			logrus.Infof("start executing job: %s", ctx.Get("id"))
			time.Sleep(10 * time.Second)
			logrus.Infof("job finished: %s", ctx.Get("id"))
		})

		var response struct {
			JobId string
		}

		response.JobId = jobId

		logrus.Infof("%v", response)

		return ctx.JSON(response)
	})

	app.Config = &aero.Configuration{
		Ports: aero.PortConfiguration{
			HTTP: getPort(),
		},
	}

	app.Run()
	cancel()
	workerPool.WaitToClose()
}
