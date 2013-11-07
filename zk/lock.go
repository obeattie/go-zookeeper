package zk

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

var (
	ErrDeadlock     = errors.New("zk: trying to acquire a lock twice")
	ErrNotLocked    = errors.New("zk: not locked")
	LocksInProgress = 0
)

type Lock struct {
	c        *Conn
	path     string
	acl      []ACL
	lockPath string
	seq      int
}

func NewLock(c *Conn, path string, acl []ACL) *Lock {
	return &Lock{
		c:    c,
		path: path,
		acl:  acl,
	}
}

func parseSeq(path string) (int, error) {
	parts := strings.Split(path, "-")
	return strconv.Atoi(parts[len(parts)-1])
}

func (l *Lock) Lock() error {
	rand.Seed(time.Now().UTC().UnixNano())
	randNum := rand.Intn(1000)

	LocksInProgress++
	log.Printf("MODDIE: Locks in progress %d\n", LocksInProgress)

	if l.c.State() != StateHasSession {
		return fmt.Errorf("MODDIE: State does not have session: %v", l.path)
	}

	log.Printf("MODDIE %v: 1\n", randNum)

	if l.lockPath != "" {
		return ErrDeadlock
	}

	log.Printf("MODDIE %v: 2\n", randNum)

	prefix := fmt.Sprintf("%s/lock-", l.path)

	path := ""
	var err error
	for i := 0; i < 3; i++ {
		path, err = l.c.CreateProtectedEphemeralSequential(prefix, []byte{}, l.acl)
		if err == ErrNoNode {
			// Create parent node.
			parts := strings.Split(l.path, "/")
			pth := ""
			for _, p := range parts[1:] {
				pth += "/" + p
				_, err := l.c.Create(pth, []byte{}, 0, l.acl)
				if err != nil && err != ErrNodeExists {
					return err
				}
			}
		} else if err == nil {
			break
		} else {
			LocksInProgress--
			return err
		}
	}
	if err != nil {
		log.Printf("MODDIE %v: 3\n", randNum)
		LocksInProgress--
		return err
	}

	log.Printf("MODDIE %v: 4\n", randNum)
	seq, err := parseSeq(path)
	if err != nil {
		log.Printf("MODDIE %v: 5\n", randNum)
		LocksInProgress--
		return err
	}

	for {
		log.Printf("MODDIE %v: 6\n", randNum)

		children, _, err := l.c.Children(l.path)
		if err != nil {
			LocksInProgress--
			return err
		}

		lowestSeq := seq
		prevSeq := 0
		prevSeqPath := ""
		log.Printf("MODDIE %v: 7\n", randNum)
		for _, p := range children {
			s, err := parseSeq(p)
			if err != nil {
				LocksInProgress--
				return err
			}
			if s < lowestSeq {
				lowestSeq = s
			}
			if s < seq && s > prevSeq {
				prevSeq = s
				prevSeqPath = p
			}
		}

		if seq == lowestSeq {
			log.Printf("MODDIE %v: lock acquired\n", randNum)
			// Acquired the lock
			break
		}

		log.Printf("MODDIE %v: 8\n", randNum)
		// Wait on the node next in line for the lock
		_, _, ch, err := l.c.GetW(l.path + "/" + prevSeqPath)
		if err != nil && err != ErrNoNode {
			log.Printf("MODDIE %v: 9\n", randNum)
			LocksInProgress--
			return err
		} else if err != nil && err == ErrNoNode {
			log.Printf("MODDIE %v: 10\n", randNum)
			// try again
			continue
		}

		log.Printf("MODDIE %v: about to listen for event: %v\n", randNum, seq)
		ev := <-ch
		log.Printf("MODDIE %v: got event event: %v\n", randNum, seq)

		if ev.Err != nil {
			log.Printf("MODDIE %v: 11\n", randNum)
			LocksInProgress--
			return ev.Err
		}
	}

	log.Printf("MODDIE %v: 12\n", randNum)
	l.seq = seq
	l.lockPath = path
	LocksInProgress--
	return nil
}

func (l *Lock) Unlock() error {
	if l.lockPath == "" {
		return ErrNotLocked
	}
	if err := l.c.Delete(l.lockPath, -1); err != nil {
		return err
	}
	l.lockPath = ""
	l.seq = 0
	return nil
}
