package stAl

import (
	logging "chimera/internal/logger"
	"chimera/internal/models/base"
	gocql "chimera/pkg/cql"
	"context"
	"errors"
	"runtime"
	"sync"
	"time"
)

// Pool Goroutine Pool
type PoolCassandra struct {
	queue chan *gocql.CqlSession
	wg    *sync.WaitGroup
	cores int
	CtxP  context.Context //要调用的父进程的Context
}

// New 新建一个协程池
func NewPool(size int, CtxP context.Context) *PoolCassandra {
	if size <= 0 {
		size = 1
	}
	return &PoolCassandra{
		queue: make(chan *gocql.CqlSession, size),
		wg:    &sync.WaitGroup{},
		CtxP:  CtxP,
	}
}

// Add 新增一个执行
func (p *PoolCassandra) Add(delta int, qe *gocql.CqlSession) {
	p.wg.Add(delta) //这个一定要放最前面，否则Done()容易报 sync: negative WaitGroup counter,
	// delta为正数就添加
	for i := 0; i < delta; i++ {
		select {
		case <-p.CtxP.Done():
			return
		default:
			p.queue <- qe
		}
	}
	// delta为负数就减少
	//for i := 0; i > delta; i-- {
	//	<-p.queue
	//}
}

// Done 执行完成减一
func (p *PoolCassandra) Done() (*gocql.CqlSession, error) {
	var qe *gocql.CqlSession
	select {
	case <-p.CtxP.Done():
		return nil, errors.New("poolCassandra no qe")
	default:
		qe = <-p.queue
	}
	p.wg.Done()
	return qe, nil
}

// Wait 等待Goroutine执行完毕
func (p *PoolCassandra) Wait() {
	for i := 0; i < p.cores; i++ {
		qe, err := p.Done()
		if err == nil {
			qe.Close()
		}
	}
	p.wg.Wait()
}

// NewPoolCassandra 生成cassandra连接池，我已优化充分测试过的，代码上下文要极严格才不会出一点错，不要照搬原作者的代码，他的有bug. 原作者的参考说明地址 https://blog.csdn.net/weixin_42445362/article/details/124607185
func NewPoolCassandra(lifetime time.Duration, coreNum int, CtxP context.Context) *PoolCassandra {
	// 这里限制为CPU内核数
	cores := runtime.NumCPU()
	if coreNum < cores {
		cores = coreNum
	}
	pool := NewPool(cores, CtxP)
	pool.cores = cores
	for i := 0; i < cores; i++ {
		qe, err := base.GocqlQeTimeOut(lifetime)
		if err != nil {
			logging.ZapLog.Info(err.Error())
			return pool
		}
		pool.Add(1, &qe)
	}
	return pool
}

//func TestPool22() {
//	// 这里限制为CPU内核数
//	cores := runtime.NumCPU()
//	pool := NewPool(cores)
//	fmt.Println("the NumGoroutine begin is:", runtime.NumGoroutine())
//	for i := 0; i < 20; i++ {
//		pool.Add(1)
//		go func(i int) {
//			time.Sleep(time.Second)
//			fmt.Println("the currentNum is:", i)
//			fmt.Println("the NumGoroutine continue is:", runtime.NumGoroutine())
//			pool.Done()
//		}(i)
//	}
//	pool.Wait()
//	fmt.Println("the NumGoroutine done is:", runtime.NumGoroutine())
//}
