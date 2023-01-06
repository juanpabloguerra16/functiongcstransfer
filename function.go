package function1

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
)

func init() {
	functions.HTTP("Serving Transfer Function", DataTransfer)
}

// export FUNCTION_TARGET=DataTransfer
func DataTransfer(w http.ResponseWriter, r *http.Request){
	
	
	bucket := r.Header.Get("bucket")
	object := r.Header.Get("object")
	containerName := r.Header.Get("containerName")
	
	var b bytes.Buffer
	by, err := TransferObjectGCStoAzure(&b, bucket, object, containerName)
	if err != nil {
		fmt.Fprintln(w, "error:", err)
	}
	if by != nil {
		fmt.Fprintln(w, "<h1>Function Processed Successfully!</h1>")
	}
	fmt.Fprintln(w, "<h1>Function Processed Successfully!</h1>")
}



// downloadFileIntoMemory downloads an object.
// download from a GCS object to a process
func TransferObjectGCStoAzure(w io.Writer, bucket, object string, containerName string) ([]byte, error) {
	// bucket := "project-1-354014"
	// object := "image.png"
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
			return nil, fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
			return nil, fmt.Errorf("Object(%q).NewReader: %v", object, err)
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
			return nil, fmt.Errorf("ioutil.ReadAll: %v", err)
	}
	fmt.Fprintf(w, "Blob %v downloaded.\n", object)

	//Azure
	// export AZURE_STORAGE_ACCOUNT_NAME=storageazjp
	accountName, ok := os.LookupEnv("AZURE_STORAGE_ACCOUNT_NAME")
	if !ok {
		panic("AZURE_STORAGE_ACCOUNT_NAME could not be found")
	}

	// export AZURE_STORAGE_PRIMARY_ACCOUNT_KEY=secret
	accountKey, ok := os.LookupEnv("AZURE_STORAGE_PRIMARY_ACCOUNT_KEY")
	if !ok {
		panic("AZURE_STORAGE_PRIMARY_ACCOUNT_KEY could not be found")
	}
	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, fmt.Errorf("%v", err)
	}

	// The service URL for blob endpoints is usually in the form: http(s)://<account>.blob.core.windows.net/
	azclient, err := azblob.NewClientWithSharedKeyCredential(fmt.Sprintf("https://%s.blob.core.windows.net/", accountName), cred, nil)
	if err != nil {
		return nil, fmt.Errorf("ioutil.ReadAll: %v", err)
	}

	uploadResp, err := azclient.UploadBuffer(context.TODO(), containerName, object, data, nil)
	if err != nil {
		return nil, fmt.Errorf("%v", err)
	}
	fmt.Println(uploadResp)

	return data, nil
}

