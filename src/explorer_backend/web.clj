(ns explorer-backend.web
  (:require
    [explorer-backend.global :as g]
    [explorer-backend.db :as db]
    [aleph.http :as http]
    [mount.core :as mount]
    [taoensso.timbre :as log]
    [compojure.core :refer [routes GET POST]]
    [ring.middleware.reload :refer [wrap-reload]]
    [ring.middleware.params :refer [wrap-params]]
    [ring.middleware.multipart-params :refer [wrap-multipart-params]]
    [ring.middleware.keyword-params :refer [wrap-keyword-params]]
    [ring.middleware.json :refer [wrap-json-body]]
    [ring.middleware.cors :refer [wrap-cors]])
  (:use [explorer-backend.util]))

(defn dissoc-all
  "删除map中的冗余的原始值"
  [mp]
  (dissoc mp :fees
    :sealInputsAddress :sealInputsAmount :sealOutputsAddress :sealOutputsAmount
    :goldInputsAddress :goldInputsAmount :goldOutputsAddress :goldOutputsAmount
    :dollarInputsAddress :dollarInputsAmount :dollarOutputsAddress :dollarOutputsAmount
    :accountInputAddress :accountOutputAddress :accountInputAmtSeal :accountOutputAmtSeal
    :accountInputAmtGold :accountOutputAmtGold :accountInputAmtDollar :accountOutputAmtDollar
    :accountSealInputs :accountSealOutputs :accountGoldInputs :accountGoldOutputs
    :accountDollarInputs :accountDollarOutputs :goldReason :dollarReason))

(defn supply-seal
  "seal币发行量"
  []
  (let [rows (db/query-supply-seal)]
    (build-right-res (:amount rows))))

(defn supply-gold
  "黄金币发行量"
  []
  (let [rows (db/query-supply-gold)]
    (build-right-res (:amount rows))))

(defn supply-dollar
  "稳定币发行量"
  []
  (let [rows (db/query-supply-dollar)]
    (build-right-res (map :amount rows))))

(defn blocks-pages
  "获取最近生成的块记录"
  [req]
  (let [{:keys [page pageSize]} (:params req)
        rows (db/query-blocks-pages pageSize page)
        t-rows (build-rows rows)
        time-rows (map #(date->timestamp % :cbeTimeIssued) t-rows)
        sum-rows (map #(merge % (build-sum % :fees :cbeFees "fee")
                              (build-sum-last % :sealOutputsAmount :accountOutputAmtSeal :cbeTotalSent "seal")
                              (build-sum-last % :goldOutputsAmount :accountOutputAmtGold :cbeTotalSentGold "gold")
                              (build-sum-last % :dollarOutputsAmount :accountOutputAmtDollar :cbeTotalSentDollar "dollar")) time-rows)
        all-rows (map #(dissoc-all %) sum-rows)
        page-flag (if (or (nil? page) (nil? pageSize)) false true)
        count (if page-flag (db/query-blocks-pages-count) (count rows))
        result [count all-rows]]
    (build-right-res result)))

(defn txs-last
  "获取最近的交易记录"
  [req]
  (let [{:keys [page pageSize]} (:params req)
        rows (db/query-txs-last pageSize page)
        t-rows (build-rows rows)
        time-rows (map #(date->timestamp % :cteTimeIssued) t-rows)
        sum-rows (map #(merge % (build-sum % :fees :cteFees "fee")
                              (build-sum-last % :sealOutputsAmount :accountOutputAmtSeal :cteAmount "seal")
                              (build-sum-last % :goldOutputsAmount :accountOutputAmtGold :cteGoldAmount "gold")
                              (build-sum-last % :dollarOutputsAmount :accountOutputAmtDollar :cteDollarAmount "dollar")) time-rows)
        all-rows (map #(dissoc-all %) sum-rows)
        page-flag (if (or (nil? page) (nil? pageSize)) false true)
        count (if page-flag (db/query-txs-last-count) (count rows))
        result [count all-rows]]
    (build-right-res result)))

(defn epochs
  "块查询"
  [epoch slot]
  (let [rows (db/query-epochs epoch slot)
        t-rows (build-rows rows)
        time-rows (map #(date->timestamp % :cbeTimeIssued) t-rows)
        sum-rows (map #(merge % (build-sum % :fees :cbeFees "fee")
                              (build-sum-2 % :sealOutputsAmount :accountOutputAmtSeal :cbeTotalSent "seal")
                              (build-sum-2 % :goldOutputsAmount :accountOutputAmtGold :cbeTotalSentGold "gold")
                              (build-sum-2 % :dollarOutputsAmount :accountOutputAmtDollar :cbeTotalSentDollar "dollar")) time-rows)
        result (map #(dissoc-all %) sum-rows)]
    (build-res result)))

(defn txs-hash
  "块交易记录"
  [hash]
  (let [rows (db/query-txs-hash hash)
        t-rows (build-rows rows)
        time-rows (map #(date->timestamp % :ctbTimeIssued) t-rows)
        fmt-rows (map #(merge %
                              (build-format-data % :sealInputsAddress :sealInputsAmount :ctbInputs)
                              (build-format-data % :sealOutputsAddress :sealOutputsAmount :ctbOutputs)
                              (build-format-data % :goldInputsAddress :goldInputsAmount :ctbGoldInputs)
                              (build-format-data % :goldOutputsAddress :goldOutputsAmount :ctbGoldOutputs)
                              (build-format-data % :dollarInputsAddress :dollarInputsAmount :ctbDollarInputs)
                              (build-format-data % :dollarOutputsAddress :dollarOutputsAmount :ctbDollarOutputs)

                              (build-format-data % :accountInputAddress :accountInputAmtSeal :accountSealInputs)
                              (build-format-data % :accountOutputAddress :accountOutputAmtSeal :accountSealOutputs)
                              (build-format-data % :accountInputAddress :accountInputAmtGold :accountGoldInputs)
                              (build-format-data % :accountOutputAddress :accountOutputAmtGold :accountGoldOutputs)
                              (build-format-data % :accountInputAddress :accountInputAmtDollar :accountDollarInputs)
                              (build-format-data % :accountOutputAddress :accountOutputAmtDollar :accountDollarOutputs)) time-rows)
        acc-rows (map #(merge %
                              (build-merge-array (:ctbInputs %) (:accountSealInputs %) :ctbInputs)
                              (build-merge-array (:ctbOutputs %) (:accountSealOutputs %) :ctbOutputs)
                              (build-merge-array (:ctbGoldInputs %) (:accountGoldInputs %) :ctbGoldInputs)
                              (build-merge-array (:ctbGoldOutputs %) (:accountGoldOutputs %) :ctbGoldOutputs)
                              (build-merge-array (:ctbDollarInputs %) (:accountDollarInputs %) :ctbDollarInputs)
                              (build-merge-array (:ctbDollarOutputs %) (:accountDollarOutputs %) :ctbDollarOutputs)) fmt-rows)
        sum-rows (map #(merge %
                              (build-sum-2 % :sealInputsAmount :accountInputAmtSeal :ctbInputSum "seal")
                              (build-sum-2 % :sealOutputsAmount :accountOutputAmtSeal :ctbOutputSum "seal")) acc-rows)
        result (map #(dissoc-all %) sum-rows)]
    (build-right-res result)))

(defn blocks-summary
  "块详情页"
  [hash]
  (let [rows (db/query-blocks-summary hash)
        t-rows (build-rows rows)
        time-rows (map #(date->timestamp % :cbeTimeIssued) t-rows)
        sum-rows (map #(merge %
                              (build-sum % :fees :cbeFees "fee")
                              (build-sum-last % :sealOutputsAmount :accountOutputAmtSeal :cbeTotalSent "seal")
                              (build-sum-last % :goldOutputsAmount :accountOutputAmtGold :cbeTotalSentGold "gold")
                              (build-sum-last % :dollarOutputsAmount :accountOutputAmtDollar :cbeTotalSentDollar "dollar")) time-rows)
        sub-rows (map #(dissoc-all %) sum-rows)
        result (map #(merge {:cbsEntry (dissoc % :cbsPrevHash :cbsNextHash :cbsMerkleRoot)}
                       (select-keys % [:cbsPrevHash :cbsNextHash :cbsMerkleRoot])) sub-rows)]
    (build-right-res (first result))))

(defn txs-summary
  "交易详情页"
  [txid]
  (let [rows (db/query-txs-summary txid)
        t-rows (build-rows rows)
        time-rows (map #(-> (date->timestamp (date->timestamp % :ctsTxTimeIssued) :ctsBlockTimeIssued)) t-rows)
        fmt-rows (map #(merge %
                              (build-format-data % :sealInputsAddress :sealInputsAmount :ctsInputs)
                              (build-unlock % :sealOutputsAddress :sealOutputsAmount :ctsId :ctsOutputs)
                              (build-format-data % :goldInputsAddress :goldInputsAmount :ctsGoldInputs)
                              (build-unlock % :goldOutputsAddress :goldOutputsAmount :ctsId :ctsGoldOutputs)
                              (build-format-data % :dollarInputsAddress :dollarInputsAmount :ctsDollarInputs)
                              (build-unlock % :dollarOutputsAddress :dollarOutputsAmount :ctsId :ctsDollarOutputs)

                              (build-format-data % :accountInputAddress :accountInputAmtSeal :accountSealInputs)
                              (build-unlock % :accountOutputAddress :accountOutputAmtSeal :ctsId :accountSealOutputs)
                              (build-format-data % :accountInputAddress :accountInputAmtGold :accountGoldInputs)
                              (build-unlock % :accountOutputAddress :accountOutputAmtGold :ctsId :accountGoldOutputs)
                              (build-format-data % :accountInputAddress :accountInputAmtDollar :accountDollarInputs)
                              (build-unlock % :accountOutputAddress :accountOutputAmtDollar :ctsId :accountDollarOutputs)) time-rows)
        acc-rows (map #(merge %
                              (build-merge-array (:ctsInputs %) (:accountSealInputs %) :ctsInputs)
                              (build-merge-array (:ctsOutputs %) (:accountSealOutputs %) :ctsOutputs)
                              (build-merge-array (:ctsGoldInputs %) (:accountGoldInputs %) :ctsGoldInputs)
                              (build-merge-array (:ctsGoldOutputs %) (:accountGoldOutputs %) :ctsGoldOutputs)
                              (build-merge-array (:ctsDollarInputs %) (:accountDollarInputs %) :ctsDollarInputs)
                              (build-merge-array (:ctsDollarOutputs %) (:accountDollarOutputs %) :ctsDollarOutputs)) fmt-rows)
        token-rows (map #(merge %
                                (build-tokens % :tokenInputs "in")
                                (build-tokens % :tokenOutputs "out")) acc-rows)
        sum-rows (map #(merge %
                              (build-sum % :fees :ctsFees "fee")
                              (build-sum-2 % :sealInputsAmount :accountInputAmtSeal :ctsTotalInput "seal")
                              (build-sum-2 % :sealOutputsAmount :accountOutputAmtSeal :ctsTotalOutput "seal")
                              (build-sum-2 % :goldOutputsAmount :accountOutputAmtGold :ctsTotalGold "gold")
                              (build-sum-2 % :dollarOutputsAmount :accountOutputAmtDollar :ctsTotalDollar "dollar")) token-rows)
        gold-state-rows (map #(merge %
                                (build-state-dollar % :sealInputsAddress :issuedGolds :goldReason :ctsSealStateInput)
                                (build-state-dollar % :sealOutputsAddress :issuedGolds :goldReason :ctsSealStateOutput)) sum-rows)
        dollar-state-rows (map #(merge %
                                (build-state-dollar % :sealInputsAddress :issuedDollars :dollarReason :ctsSealStateInput)
                                (build-state-dollar % :sealOutputsAddress :issuedDollars :dollarReason :ctsSealStateOutput)) sum-rows)
        state-rows (if (= "G" (db/issues-flag txid)) gold-state-rows dollar-state-rows)
        result (map #(dissoc-all %) state-rows)]
    (build-right-res (first result))))

(defn issue-gold
  "黄金币发行记录"
  []
  (let [rows (db/query-issue-gold)
        t-rows (build-rows rows)
        time-rows (map #(-> (date->timestamp (date->timestamp % :ctsTxTimeIssued) :ctsBlockTimeIssued)) t-rows)
        fmt-rows (map #(merge %
                              (build-format-data % :sealInputsAddress :sealInputsAmount :ctsInputs)
                              (build-format-data % :sealOutputsAddress :sealOutputsAmount :ctsOutputs)
                              (build-format-data % :goldInputsAddress :goldInputsAmount :ctsGoldInputs)
                              (build-format-data % :goldOutputsAddress :goldOutputsAmount :ctsGoldOutputs)) time-rows)
        sum-rows (map #(merge %
                              (build-sum % :fees :ctsFees "fee")
                              {:ctsIssuedGolds {:getCGoldCoin (str (get % :issuedGolds))}}
                              {:ctsDestroyedGolds {:getCGoldCoin (str (get % :destoryedGolds))}}) fmt-rows)
        state-rows (map #(merge %
                                (build-state-gold % :sealInputsAddress :issuedGolds :reason :ctsSealStateInput)
                                (build-state-gold % :sealOutputsAddress :issuedGolds :reason :ctsSealStateOutput)) sum-rows)
        result (map #(dissoc-all %) state-rows)]
    (build-right-res result)))

(defn issue-dollar
  "稳定币发行记录"
  []
  (let [
        rows (db/query-issue-dollar)
        t-rows (build-rows rows)
        time-rows (map #(-> (date->timestamp (date->timestamp % :ctsTxTimeIssued) :ctsBlockTimeIssued)) t-rows)
        fmt-rows (map #(merge %
                              (build-format-data % :sealInputsAddress :sealInputsAmount :ctsInputs)
                              (build-format-data % :sealOutputsAddress :sealOutputsAmount :ctsOutputs)
                              (build-format-data % :goldInputsAddress :goldInputsAmount :ctsGoldInputs)
                              (build-format-data % :goldOutputsAddress :goldOutputsAmount :ctsGoldOutputs)
                              (build-format-data % :dollarInputsAddress :dollarInputsAmount :ctsDollarInputs)
                              (build-format-data % :dollarOutputsAddress :dollarOutputsAmount :ctsDollarOutputs)) time-rows)
        sum-rows (map #(merge %
                              (build-sum % :fees :ctsFees "fee")
                              {:ctsIssuedDollars {:getCGoldDollar (str (get % :issuedDollars))}}
                              {:ctsDestroyedDollars {:getCGoldDollar (str (get % :destoryedDollars))}}) fmt-rows)
        state-rows (map #(merge %
                                (build-state-dollar % :sealInputsAddress :issuedDollars :reason :ctsSealStateInput)
                                (build-state-dollar % :sealOutputsAddress :issuedDollars :reason :ctsSealStateOutput)) sum-rows)
        result (map #(dissoc-all %) state-rows)]
    (build-right-res result)))

(defn addresses-summary
  "地址详情页"
  [address]
  (let [amount-rows (db/query-addresses-summary-amt address)
        tx-rows (db/query-addresses-summary-tx address)
        t-rows (build-rows tx-rows)
        time-rows (map #(date->timestamp % :ctsTxTimeIssued) t-rows)
        fmt-rows (map #(merge %
                              (build-format-data % :sealInputsAddress :sealInputsAmount :ctbInputs)
                              (build-format-data % :sealOutputsAddress :sealOutputsAmount :ctbOutputs)
                              (build-format-data % :goldInputsAddress :goldInputsAmount :ctbGoldInputs)
                              (build-format-data % :goldOutputsAddress :goldOutputsAmount :ctbGoldOutputs)
                              (build-format-data % :dollarInputsAddress :dollarInputsAmount :ctbDollarInputs)
                              (build-format-data % :dollarOutputsAddress :dollarOutputsAmount :ctbDollarOutputs)

                              (build-format-data % :accountInputAddress :accountInputAmtSeal :accountSealInputs)
                              (build-format-data % :accountOutputAddress :accountOutputAmtSeal :accountSealOutputs)
                              (build-format-data % :accountInputAddress :accountInputAmtGold :accountGoldInputs)
                              (build-format-data % :accountOutputAddress :accountOutputAmtGold :accountGoldOutputs)
                              (build-format-data % :accountInputAddress :accountInputAmtDollar :accountDollarInputs)
                              (build-format-data % :accountOutputAddress :accountOutputAmtDollar :accountDollarOutputs)) time-rows)
        acc-rows (map #(merge %
                              (build-merge-array (:ctbInputs %) (:accountSealInputs %) :ctbInputs)
                              (build-merge-array (:ctbOutputs %) (:accountSealOutputs %) :ctbOutputs)
                              (build-merge-array (:ctbGoldInputs %) (:accountGoldInputs %) :ctbGoldInputs)
                              (build-merge-array (:ctbGoldOutputs %) (:accountGoldOutputs %) :ctbGoldOutputs)
                              (build-merge-array (:ctbDollarInputs %) (:accountDollarInputs %) :ctbDollarInputs)
                              (build-merge-array (:ctbDollarOutputs %) (:accountDollarOutputs %) :ctbDollarOutputs)) fmt-rows)
        filter-rows (filter #(useful-tx? %) acc-rows)
        token-rows (map #(merge %
                                (build-tokens % :tokenInputs "in")
                                (build-tokens % :tokenOutputs "out")) filter-rows)
        sum-rows (map #(merge %
                              (build-sum % :fees :ctsFees "fee")
                              (build-sum-2 % :sealInputsAmount :accountInputAmtSeal :ctbInputSum "seal")
                              (build-sum-2 % :sealOutputsAmount :accountOutputAmtSeal :ctbOutputSum "seal")
                              (build-sum-2 % :goldOutputsAmount :accountOutputAmtGold :ctbTotalGold "gold")
                              (build-sum-2 % :dollarOutputsAmount :accountOutputAmtDollar :ctbTotalDollar "dollar")) token-rows)
        tx-result (map #(dissoc-all %) sum-rows)
        ca-addr {:caAddress address}
        ca-tx-num {:caTxNum (count filter-rows)}
        seal-amt (build-balance "seal" (:seal_sum amount-rows))
        gold-amt (build-balance "gold" (:gold_sum amount-rows))
        dollar-amt (build-balance "dollar" (:dollar_sum amount-rows))
        ca-tx-list {:caTxList tx-result}
        tokens {:tokens (db/query-addresses-summary-token address)}
        result (merge ca-addr ca-tx-num seal-amt gold-amt dollar-amt ca-tx-list tokens)]
    (build-right-res result)))

(defn token-list
  "查看Token列表"
  [req]
  (let [{:keys [page-size page-now]} (:body req)
        rows (db/query-token-list page-size page-now)
        time-rows (map #(date->timestamp % :create_time) rows)]
    (build-right-res (build-rows time-rows))))

(defn token-detail
  "查看Token详情"
  [req]
  (let [{:keys [token-id]} (:body req)
        rows (db/query-token-detail token-id)
        time-rows (map #(date->timestamp % :create_time) rows)]
    (build-right-res (build-rows time-rows))))

(defn token-issuance
  "查看Token发行记录"
  [req]
  (let [{:keys [token-id page-size page-now]} (:body req)
        rows (build-rows (db/query-token-issuance token-id page-size page-now))
        time-rows (map #(date->timestamp % :time) rows)
        total (:count (db/query-token-issuance-count token-id))
        result (build-page-info time-rows total page-size page-now)]
    (build-right-res result)))

(defn token-holders
  "查看Token的持有人"
  [req]
  (let [{:keys [token-id page-size page-now]} (:body req)
        rows (db/query-token-holders token-id page-size page-now)
        addr-rows (build-rows (map #(merge % (build-addr-type token-id (:address %))) rows))
        total (:count (db/query-token-holders-count token-id))
        result (build-page-info addr-rows total page-size page-now)]
    (build-right-res result)))

(defn token-profit
  "查看Token的分红记录"
  [req]
  (let [{:keys [token-id page-size page-now]} (:body req)
        rows (build-rows (db/query-token-profit token-id page-size page-now))
        time-rows (map #(date->timestamp % :time) rows)
        total (:count (db/query-token-profit-count token-id))
        result (build-page-info time-rows total page-size page-now)]
    (build-right-res result)))

(def http-handler
  (-> (routes
        (GET "/api/supply/seal" [] (supply-seal))
        (GET "/api/supply/gold" [] (supply-gold))
        (GET "/api/supply/dollar" [] (supply-dollar))
        (GET "/api/blocks/pages" [] blocks-pages)
        (GET "/api/txs/last" [] txs-last)
        (GET "/api/epochs/:epoch/:slot" [epoch slot] (epochs epoch slot))
        (GET "/api/blocks/txs/:hash" [hash] (txs-hash hash))
        (GET "/api/blocks/summary/:hash" [hash] (blocks-summary hash))
        (GET "/api/txs/summary/:txid" [txid] (txs-summary txid))
        (GET "/api/addresses/summary/:address" [address] (addresses-summary address))
        (GET "/api/issue/gold" [] (issue-gold))
        (GET "/api/issue/dollar" [] (issue-dollar))
        (POST "/api/token/list" [] token-list)
        (POST "/api/token/detail" [] token-detail)
        (POST "/api/token/issuance" [] token-issuance)
        (POST "/api/token/holders" [] token-holders)
        (POST "/api/token/profit" [] token-profit))
      ;; 解决跨域问题
      (wrap-cors :access-control-allow-origin [#".*"]
                 :access-control-allow-headers ["Content-Type"]
                 :access-control-allow-methods [:get :put :post :delete :options])
      ;; 用于代码修改的时候自动编译并重启http服务
      (wrap-reload)
      (wrap-keyword-params)
      (wrap-params)
      (wrap-multipart-params)
      (wrap-json-body {:keywords? true :bigdecimals? true})))

(mount/defstate http-server
  :start (let [port (:http-port @g/config)
               server (http/start-server http-handler {:port port})]
           (log/info (str "http server started on port: " port "."))
           server)
  :stop (do
          (.close http-server)
          (log/info "http server stopped.")))