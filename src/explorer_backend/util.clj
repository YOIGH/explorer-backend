(ns explorer-backend.util
  (:require
    [explorer-backend.db :as db]
    [ring.util.json-response :refer [json-response]]
    [camel-snake-kebab.core :as trans]
    [clojure.string :as string])
  (:import (org.postgresql.jdbc PgArray)))

(defn mk-array
  "格式化数组查询"
  [addresses]
  (str "array['" (string/join "','" addresses) "']"))

(defn   date->timestamp
  "将coll里的指定属性转换为时间戳"
  [mp prop]
  (let [prop-val (get mp prop)
        times (if (nil? prop-val) 0 (/ (.getTime prop-val) 1000))
        result (assoc mp prop times)]
    result))

(defn pgArray->vector
  "pg数组转换为Vector"
  [obj]
  (if (instance? PgArray obj)
    (vec (.getArray obj))
    obj))

(defn serialize-pg-map
  "将map里所有的pg数组转换为Vector"
  [mp]
  (into {} (for [[k v] mp]
             [k (pgArray->vector v)])))

(defn build-right-res
  "成功：为所有的response加上Right标识"
  [arg]
  (json-response {:Right arg}))

(defn build-left-res
  "失败：为所有的response加上Left标识"
  [arg]
  (json-response {:Left arg}))

(defn build-res
  "为所有的response加上标识"
  [arg]
  (if (= 0 (count arg))
    (build-left-res arg)
    (build-right-res arg)))

(defn underline-to-camel
  "将map中的key转换为驼峰命名"
  [mp]
  (into (empty mp)
        (for [[k v] mp]
          [(keyword (trans/->camelCase (name k))) v])))

(defn build-rows
  "数组+驼峰命名转换"
  [rows]
  (map underline-to-camel (map serialize-pg-map rows)))

(defn build-format-data
  "将map中指定的地址和金额转换为旧格式"
  [mp addr amt prop]
  (let [addresses (get mp addr)
        amounts (get mp amt)
        t-data (if (or (empty? amounts) (= 0 (get amounts 0)))
                 []
                 (for [x (range 0 (count addresses))]
                   (list (get addresses x) {:getCoin (str (get amounts x))})))
        result {prop t-data}]
    result))

(defn build-unlock
  "构建解锁时间"
  [mp addr amt hash prop]
  (let [addresses (get mp addr)
        amounts (get mp amt)
        count (count addresses)
        hash (get mp hash)
        unlock-time (db/query-unlock-time hash (mk-array addresses))
        t-data (if (or (empty? amounts) (= 0 (get amounts 0)))
                 []
                 (for [x (range 0 count)]
                   (list (get addresses x)
                         {:getCoin (str (get amounts x))}
                         {:unlockTime (if (and (< 1 count) (= (- count 1) x))
                                        "" unlock-time)})
                   )) ;; 找零交易不需要解锁时间
        result {prop t-data}]
    result))

(defn build-sum
  "计算指定属性并转为旧格式"
  [mp prop key type]
  (let [prop-val (get mp prop)
        sum (str (if (= "fee" type)
                   (if (nil? prop-val) 0 prop-val)
                   (if (nil? prop-val) 0 (apply + prop-val))))
        result (case type
                 "fee" {key {:getCoin sum}}
                 "seal" {key {:getCoin sum}}
                 "gold" {key {:getCGoldCoin sum}}
                 "dollar" {key {:getCGoldDollar sum}})]
    result))

(defn build-sum-2
  "计算指定属性并转为旧格式"
  [mp prop prop-2 key type]
  (let [prop-val (get mp prop)
        prop-val-2 (get mp prop-2)
        sum (str (if (= "fee" type)
                   (if (nil? prop-val) 0 prop-val)
                   (if (nil? prop-val) 0 (+ (apply + prop-val) (apply + prop-val-2)))))
        result (case type
                 "fee" {key {:getCoin sum}}
                 "seal" {key {:getCoin sum}}
                 "gold" {key {:getCGoldCoin sum}}
                 "dollar" {key {:getCGoldDollar sum}})]
    result))

(defn nil-val?
  [val]
  (or (empty? val) (= 0 (apply + val))))

(defn build-sum-last
  "计算指定属性并转为旧格式（utxo的金额去掉找零）"
  [mp prop prop-2 key type]
  (let [prop-val-raw (get mp prop)
        count (count prop-val-raw)
        prop-val (rest (reverse prop-val-raw))
        prop-2-val (get mp prop-2)
        sum (str (cond
                   (and (= 1 count) (nil-val? prop-2-val)) (apply + prop-val-raw) ;; ACC-UTXO(count=1 && count-ACC=0)
                   (and (= 0 count) (not (nil-val? prop-2-val))) (apply + prop-2-val) ;; ACC-ACC(count=0 && count-ACC>=1)
                   (and (= 1 count) (not (nil-val? prop-2-val))) (+ (apply + prop-val) (apply + prop-2-val)) ;; UTXO-ACC(count=1 && count-ACC=1) 存在数组不为空但是里面赋值时0的情况
                   (and (< 1 count) (nil-val? prop-2-val)) (apply + prop-val) ;; UTXO-UTXO(count>1 && count-2=0)
                   :else 0))
        result (case type
                 "seal" {key {:getCoin sum}}
                 "gold" {key {:getCGoldCoin sum}}
                 "dollar" {key {:getCGoldDollar sum}})]
    result))

(defn build-balance
  "拼接余额结构"
  [type val]
  (let [balance (if (nil? val) "0" val)
        result (case type
                 "seal" {:caBalance {:getCoin balance}}
                 "gold" {:caGoldBalance {:getCGoldCoin balance}}
                 "dollar" {:caDollarBalance {:getCGoldDollar balance}})]
    result))

(defn build-state-gold
  "拼接state结构"
  [mp addr amt reason prop]
  (let [addresses (get mp addr)
        amounts (get mp amt)
        reason (get mp reason)
        t-data (for [x (range 0 (count addresses))]
                 (list (get addresses x)
                       (merge {:tag "CGoldCoinIssueCert" :reason reason}
                              {:issuedGolds {:getCGoldCoin (str amounts)}})
                       (merge {:tag "CGoldCoinState"}
                              {:totalGolds {:getCGoldCoin (str amounts)}})))
        result {prop (first t-data)}]
    result))

(defn build-state-dollar
  "拼接state结构"
  [mp addr amt reason prop]
  (let [addresses (get mp addr)
        amounts (get mp amt)
        reason (get mp reason)
        t-data (for [x (range 0 (count addresses))]
                 (list (get addresses x)
                       (merge {:tag "CGoldDollarIssueCert" :reason reason}
                              {:issuedDollars {:getCGoldDollar (str amounts)}}
                              {:lockedGolds {:getCGoldCoin (str amounts)}})
                       (merge {:tag "CGoldDollarState"}
                              {:totalDollars {:getCGoldDollar (str amounts)}}
                              {:totalLockedGolds {:getCGoldCoin (str amounts)}})))
        result {prop (first t-data)}]
    result))

(defn build-addr-type
  [token-id addr]
  (let [flag (db/owner? token-id addr)
        result (if flag
                 {:address_type "Owner"}
                 {:address_type "-"})]
    result))

(defn build-page-info
  [datas total-rows page-size page-now]
  (merge {:datas datas}
         {:totalRows total-rows}
         {:pageSize page-size}
         {:pageNow page-now}))

(defn build-merge-array
  [first second prop]
  {prop (into first second)})

(defn build-single-token
  [cmd type]
  (let [cmd-array (string/split cmd #",")
        flag (string/includes? (get cmd-array 0) "(Token/send")
        result (if flag
                 (let [from-addr (string/replace (get cmd-array 1) ":from " "")
                       to-addr (string/replace (get cmd-array 2) ":to " "")
                       amt (string/replace (string/replace (get cmd-array 3) ":amount " "") "})" "")
                       addr (if (= "in" type) from-addr to-addr)
                       amt {:getCoin amt}
                       token [addr amt]] token))] result))

(defn build-tokens
  [mp prop type]
  (let [raw-cmds (:cmd mp)
        tokens (map #(build-single-token % type) raw-cmds)
        result {prop (filter #(not (nil? %)) tokens)}]
    result))

(defn array-include?
  [array str]
  (let [counts (filter #(string/includes? % str) array)
        flag (if (= 0 (count counts)) false true)]
    flag))

;; 判断是否token-send，非空并且不是send的话需要过滤掉
(defn useful-tx?
  [mp]
  (let [cmd (:cmd mp)
        flag (if (or (empty? cmd) (array-include? cmd "(Token/send")) true false)]
    flag))