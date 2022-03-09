package pool

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/panjf2000/ants/v2"
	"github.com/sirupsen/logrus"
)

type PointerToFunction func()

type jobToExecute struct {
	uuid string
	pf   *PointerToFunction
}

type Pool struct {
	functionQueue chan (*jobToExecute)
	pool          *ants.Pool
	wg            *sync.WaitGroup
}

func New(ctx context.Context, queueSize int, workerCount int) (*Pool, error) {
	pool, err := ants.NewPool(workerCount, ants.WithPreAlloc(true))

	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup

	p := &Pool{
		functionQueue: make(chan *jobToExecute, queueSize),
		pool:          pool,
		wg:            &wg,
	}

	go func() {
		for {
			select {
			case pf := <-p.functionQueue:
				p.appendToAntsPool(pf)

			case <-ctx.Done():
				logrus.Infof("dispatcher done.")
				p.wg.Done()
				return
			}
		}
	}()
	p.wg.Add(1)

	return p, nil
}

func (p *Pool) Push(pf PointerToFunction) string {
	jobId := uuid.NewString()
	p.functionQueue <- &jobToExecute{
		pf:   &pf,
		uuid: jobId,
	}
	return jobId
}

func (p *Pool) appendToAntsPool(job *jobToExecute) (string, error) {

	p.wg.Add(1)
	err := p.pool.Submit(func() {
		fn2Execute := *job.pf
		fn2Execute()
		p.wg.Done()
		logrus.Infof("function done: %s", job.uuid)
	})

	if err != nil {
		logrus.Error(err)
		return job.uuid, err
	}
	logrus.Infof("new function arrived: %s, function executing: %d", job.uuid, p.pool.Running())

	return job.uuid, nil
}

func (p *Pool) WaitToClose() {
	p.pool.Release()
	for {
		logrus.Infof("wait for ants done, there still some function executing: %d", p.pool.Running())
		time.Sleep(1 * time.Second)
		if p.pool.Running() == 0 {
			return
		}
	}
	p.wg.Wait()
}
