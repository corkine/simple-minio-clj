# Simple MinIO Wrapper For Clojure

Support For The Last MinIO Java Library

> io.minio/minio 8.5.7

Usage:

```bash
//deps.edn
{com.mazhangjing/simple-minio-clj 
  {:git/url "https://github.com/corkine/simple-minio-clj.git"
   :git/sha "last-sha"}}
```

```clojure
(require 'minio)
(def c (minio/connect "URL" "AK" "SK"))
(minio/list-buckets c)
(minio/make-bucket c :test-bucket)
(minio/put-object c :test-bucket "deps.edn")
(minio/list-objects c :test-bucket)
(minio/remove-bucket! c :test-bucket)
```