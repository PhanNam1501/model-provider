package fakeservice

import (
	"fmt"
	"io"
	"io/fs"
	"math"
	"math/rand"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type fakeservice struct {
	jobStatus       map[uuid.UUID]bool
	jobResult       map[uuid.UUID]int64
	jobCallCnt      map[uuid.UUID]int
	jobCallErrorCnt map[uuid.UUID]int
	jobErrors       map[uuid.UUID]string
	jobCnt          int
	mutex           sync.Mutex
}

type FakeService interface {
	uploadfile(jobId uuid.UUID, file *multipart.FileHeader, dst string, perm ...fs.FileMode) error
	Upload(c *gin.Context)
	calculate(count int64) (float64, float64, float64)
	CheckStatus(c *gin.Context)
}

func NewFakeService() FakeService {
	return &fakeservice{
		jobStatus:       map[uuid.UUID]bool{},
		jobResult:       make(map[uuid.UUID]int64),
		jobCallCnt:      make(map[uuid.UUID]int),
		jobCallErrorCnt: make(map[uuid.UUID]int),
		jobErrors:       make(map[uuid.UUID]string),
		jobCnt:          0,
	}
}

func (f *fakeservice) uploadfile(jobId uuid.UUID, file *multipart.FileHeader, dst string, perm ...fs.FileMode) error {
	src, err := file.Open()
	if err != nil {
		return err
	}
	defer src.Close()

	var mode os.FileMode = 0o750
	if len(perm) > 0 {
		mode = perm[0]
	}
	dir := filepath.Dir(dst)
	if err = os.MkdirAll(dir, mode); err != nil {
		return err
	}
	if err = os.Chmod(dir, mode); err != nil {
		return err
	}

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	written, err := io.Copy(out, src)
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if err != nil {
		f.jobErrors[jobId] = err.Error()
	} else {
		f.jobStatus[jobId] = true
		f.jobResult[jobId] = written
	}
	f.jobCnt--
	return err
}

func (f *fakeservice) Upload(c *gin.Context) {
	f.mutex.Lock()
	jobCnt := f.jobCnt
	if jobCnt > 2 {
		f.mutex.Unlock()
		retrySeconds := rand.Intn(3) + 1

		c.Header("Retry-After", strconv.Itoa(retrySeconds))

		c.JSON(http.StatusTooManyRequests, gin.H{
			"error_code": "too_many_requests",
			"message":    fmt.Sprintf("Limited job. Please retry after %d", retrySeconds),
		})
		return
	} else {
		f.jobCnt++
		f.mutex.Unlock()
	}
	file, err := c.FormFile("file")
	if err != nil {
		f.mutex.Lock()
		f.jobCnt--
		f.mutex.Unlock()
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	jobId := uuid.New()
	dst := filepath.Join("./files/", filepath.Base(file.Filename))
	go f.uploadfile(jobId, file, dst)
	message := fmt.Sprintf("job id: %s", jobId.String())
	c.String(http.StatusOK, message)
}

type ResultResponse struct {
	InputToken  float64 `json:"input-token"`
	OutputToken float64 `json:"output-token"`
	Prices      float64 `json:"prices"`
}

func (f *fakeservice) calculate(count int64) (float64, float64, float64) {
	inputToken := math.Ceil(float64(count) / 4)
	outputToken := math.Ceil(inputToken * 0.2)
	prices := inputToken*0.003 + outputToken*0.015
	return inputToken, outputToken, prices
}

func (f *fakeservice) CheckStatus(c *gin.Context) {
	id := c.Query("id")
	jobid, err := uuid.Parse(id)
	if err != nil {
		c.String(http.StatusInternalServerError, "Cannot parse jobID")
		return
	}
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.jobCallCnt[jobid]++
	if float64(f.jobCallErrorCnt[jobid]+1)/float64(f.jobCallCnt[jobid]) <= 0.15 {
		f.jobCallErrorCnt[jobid]++
		c.JSON(http.StatusInternalServerError, gin.H{
			"error_code": "rate-limit",
			"message":    "you have called many times, please hold on",
		})
		return
	}

	if errMsg, hasError := f.jobErrors[jobid]; hasError {
		delete(f.jobErrors, jobid)
		delete(f.jobStatus, jobid)
		delete(f.jobResult, jobid)
		delete(f.jobCallCnt, jobid)
		delete(f.jobCallErrorCnt, jobid)

		c.JSON(http.StatusInternalServerError, gin.H{
			"error_code": "upload_failed",
			"message":    fmt.Sprintf("Upload failed: %s", errMsg),
		})
		return
	}

	status := f.jobStatus[jobid]
	if !status {
		message := "still upload file"
		c.String(http.StatusOK, message)
	} else {
		count := f.jobResult[jobid]
		if count == 0 || count > 10000 {
			c.JSON(http.StatusBadRequest, gin.H{
				"error_code": "invalid_input",
				"message":    "Content cannot be empty or length bigger than 10000 characters",
			})
			return
		}
		delete(f.jobStatus, jobid)
		delete(f.jobResult, jobid)
		delete(f.jobCallCnt, jobid)
		delete(f.jobCallErrorCnt, jobid)
		time.Sleep(time.Duration(rand.Intn(2001)+1000) * time.Millisecond)
		inputToken, outputToken, prices := f.calculate(count)
		c.JSON(http.StatusOK, ResultResponse{
			InputToken:  inputToken,
			OutputToken: outputToken,
			Prices:      prices,
		})

	}
}
