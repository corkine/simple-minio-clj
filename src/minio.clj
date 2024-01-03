(ns minio
  (:require [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [io.minio GetObjectArgs ListObjectsArgs MakeBucketArgs MinioClient PutObjectArgs RemoveBucketArgs RemoveObjectArgs Result StatObjectArgs StatObjectResponse]
           [java.io File]
           [java.time LocalDateTime]
           [java.time.format DateTimeFormatter]
           (kotlin Pair)))

(defn connect
  [^String url ^String access-key ^String secret-key]
  (-> (MinioClient/builder)
      (.endpoint url)
      (.credentials access-key secret-key)
      (.build)))

(defn make-bucket
  "Creates a bucket with a name. Does nothing if one exists. Returns nil
   https://docs.minio.io/docs/java-client-api-reference#makeBucket
  "
  [^MinioClient conn name]
  (try
    (.makeBucket conn
                 (-> (MakeBucketArgs/builder)
                     (.bucket (clojure.core/name name))
                     (.build)))
    (catch Exception _ nil))
  name)

(defn list-buckets
  "returns maps "
  [^MinioClient conn]
  (->> conn
       (.listBuckets)
       (map (fn [bucket] {:creation-date (str (.creationDate bucket))
                          :name          (.name bucket)}))))

(defn- UUID []
  (random-uuid))

(defn- NOW []
  (-> (LocalDateTime/now)
      (.format (DateTimeFormatter/ofPattern "yMMd-HHmm"))))

(defn put-object
  "Uploads a file object to the bucket.
   Returns a map of bucket name and file name
  "
  ([^MinioClient conn bucket ^String file-name]
   (let [bucket (clojure.core/name bucket)
         upload-name (str (NOW) "_" (UUID) "_" file-name)]
     (put-object conn bucket upload-name file-name)
     {:bucket bucket
      :name   upload-name}))
  ([^MinioClient conn bucket ^String upload-name ^String source-file-name]
   (let [bucket (clojure.core/name bucket)
         ^File file (io/file source-file-name)]
     (.putObject conn
                 (-> (PutObjectArgs/builder)
                     (.stream (io/input-stream file) (.length file) -1)
                     (.object upload-name)
                     (.bucket bucket)
                     (.build))))
   {:bucket bucket
    :name   upload-name}))

(defn get-object
  "Takes connection and a map of [bucket name] keys as returned by (put-object) or explicit arguments
   returns java.io.BufferedReader.
   Use clojure.java.io/copy to stream the bucket data files, or HTTP responses
  "
  ([^MinioClient conn {:keys [bucket name]}]
   (get-object conn bucket name))
  ([^MinioClient conn bucket name]
   (.getObject conn
               (-> (GetObjectArgs/builder)
                   (.object (clojure.core/name name))
                   (.bucket (clojure.core/name bucket))
                   (.build)))))

(defn download-object
  "Download object to a local path."
  [^MinioClient conn bucket name localpath]
  (io/copy (get-object conn bucket name) (io/file localpath)))

(defn- objectStat->map
  "helper function for datatype conversion"
  [^StatObjectResponse stat]
  (println stat)
  {:bucket        (.bucket stat)
   :name          (.object stat)
   :length        (.size stat)
   :last-modified (.lastModified stat)
   :http-headers  (reduce (fn [agg ^Pair pair] (assoc agg
                                                 (keyword (str/lower-case (.component1 pair)))
                                                 (.component2 pair)))
                          {}
                          (.headers stat))})

(defn get-object-meta
  "Returns object metadata as clojure hash-map"
  ([^MinioClient conn bucket name]
   (let [query (-> (StatObjectArgs/builder)
                   (.object (clojure.core/name name))
                   (.bucket (clojure.core/name bucket))
                   (.build))]
     (-> (.statObject conn query)
         (objectStat->map))))
  ([^MinioClient conn {:keys [bucket name]}]
   (get-object-meta conn bucket name)))

(defmacro swallow-exceptions [& body]
  `(try ~@body (catch Exception e#)))

(defn- objectItem->map
  "Helper function for datatype conversion."
  [item]
  {:etag          (.etag item)
   :last-modified (swallow-exceptions (.lastModified item))
   :key           (.objectName item)
   :owner         (.owner item)
   :size          (.size item)
   :storage-class (.storageClass item)
   :user-metadata (.userMetadata item)
   :version-id    (.versionId item)})

(defn- item->map [^Result item]
  (->> (.get item)
       (objectItem->map)))

(defn list-objects
  ([^MinioClient conn bucket]
   (list-objects conn bucket "" true))
  ([^MinioClient conn bucket filter]
   (list-objects conn bucket filter true))
  ([^MinioClient conn bucket filter recursive]
   (map item->map (.listObjects conn
                                (-> (ListObjectsArgs/builder)
                                    (.recursive recursive)
                                    (.prefix filter)
                                    (.bucket (name bucket))
                                    (.build))))))

(defn remove-bucket!
  "removes the bucket form the storage"
  [^MinioClient conn bucket-name]
  (.removeBucket conn
                 (-> (RemoveBucketArgs/builder)
                     (.bucket (clojure.core/name bucket-name))
                     (.build))))

(defn remove-object! [^MinioClient conn bucket object]
  (.removeObject conn
                 (-> (RemoveObjectArgs/builder)
                     (.object (clojure.core/name object))
                     (.bucket (clojure.core/name bucket))
                     (.build))))