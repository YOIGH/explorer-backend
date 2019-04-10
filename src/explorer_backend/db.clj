(ns explorer-backend.db
  (:require
    [mount.core :as mount]
    [clojure.java.jdbc :as jdbc]
    [taoensso.timbre :as log]
    [hikari-cp.core :refer [make-datasource close-datasource]]
    [explorer-backend.global :as g]))

(mount/defstate datasource
  :start (let [options (merge
                        {:read-only          false
                         :connection-timeout 30000
                         :validation-timeout 5000
                         :idle-timeout       600000
                         :max-lifetime       1800000
                         :minimum-idle       10
                         :maximum-pool-size  10
                         :pool-name          "db-pool"
                         :adapter            "postgresql"}
                        (:db @g/config))
               ds (make-datasource options)]
           (log/info (str "Conn pool ready. " (name (:env @g/config))))
           ds)
  :stop (do
          (close-datasource datasource)
          (log/info "Conn pool closed.")))

(defn query [stmt]
  (jdbc/with-db-connection [conn {:datasource datasource}]
                           (jdbc/query conn stmt)))

(defn query-one
  [stmt] (first (query stmt)))

(defn build-page-query-row
  "按条数进行分页"
  [stmt page-size page-now]
  (let [page-size (if (nil? page-size) 10 page-size)
        page-now (if (nil? page-now) 0 page-now)
        stmt (str stmt (format " LIMIT %s OFFSET %s " page-size page-now))]
    stmt))

(defn build-page-query-page
  "按页数进行分页"
  [stmt page-size page-now]
  (let [page-size (if (nil? page-size) 10 page-size)
        page-now (if (nil? page-now) 1 page-now)
        stmt (str stmt (format " LIMIT %s OFFSET %s " page-size (* (- page-now 1) page-size)))]
    stmt))

;;
;; 业务相关查询语句
;;
(defn query-supply-seal
  []
  (let [stmt "SELECT amount FROM totals where currency = 0"
        rows (query-one stmt)]
    (log/info "===query-supply-seal===:" stmt)
    rows))

(defn query-supply-gold
  []
  (let [stmt "SELECT amount FROM totals where currency = 1"
        rows (query-one stmt)]
    (log/info "===query-supply-gold===:" stmt)
    rows))

(defn query-supply-dollar
  []
  (let [stmt "SELECT amount FROM totals where currency IN (1, 2) ORDER BY currency desc"
        rows (query stmt)]
    (log/info "===query-supply-dollar===:" stmt)
    rows))

(defn query-blocks-pages
  [page-size page-now]
  (let [stmt "SELECT
              B.epoch AS cbe_epoch,
              B.slot AS cbe_slot,
              B.hash AS cbe_blk_hash,
              B.time_issued AS cbe_time_issued,
              B.tx_num AS cbe_tx_num,
              B.size AS cbe_size,
              B.block_leader AS cbe_block_lead,
              P.fees,
              P.seal_outputs_amount,
              P.gold_outputs_amount,
              P.dollar_outputs_amount,
              account_output_amt_seal,
              account_output_amt_gold,
              account_output_amt_dollar
              FROM blocks B
              LEFT JOIN plain_txs P ON P.blk_hash = B.hash
              ORDER BY B.time_issued DESC"
        page-stmt (build-page-query-row stmt page-size page-now)
        rows (query page-stmt)]
    (log/info "===query-blocks-pages===:" page-stmt)
    rows))

(defn query-blocks-pages-count
  []
  (let [stmt "SELECT
              COUNT(1)
              FROM blocks B
              LEFT JOIN plain_txs P ON P.blk_hash = B.hash"
        result (:count (query-one stmt))]
    (log/info "===query-blocks-pages-count===:" stmt)
    result))

(defn query-txs-last
  [page-size page-now]
  (let [stmt "SELECT
              hash AS cte_id,
              time AS cte_time_issued,
              fees,
              seal_outputs_amount,
              gold_outputs_amount,
              dollar_outputs_amount,
              account_output_amt_seal,
              account_output_amt_gold,
              account_output_amt_dollar
              FROM plain_txs
              ORDER BY time DESC"
        page-stmt (build-page-query-row stmt page-size page-now)
        rows (query page-stmt)]
    (log/info "===query-txs-last===:" page-stmt)
    rows))

(defn query-txs-last-count
  []
  (let [stmt "SELECT
              COUNT(1)
              FROM plain_txs"
        result (:count (query-one stmt))]
    (log/info "===query-txs-last-count===:" stmt)
    result))

(defn query-epochs
  [epoch slot]
  (let [stmt "SELECT T.epoch AS cbe_epoch,
              T.slot AS cbe_slot,
              T.hash AS cbe_blk_hash,
              T.time_issued AS cbe_time_issued,
              T.tx_num AS cbe_tx_num,
              T.size AS cbe_size,
              T.block_leader AS cbe_block_lead,
              P.seal_outputs_amount,
              P.gold_outputs_amount,
              P.dollar_outputs_amount,
              P.fees as cbe_fees,
              P.account_output_amt_seal,
              P.account_output_amt_gold,
              P.account_output_amt_dollar
              FROM blocks T
              LEFT JOIN plain_txs P ON P.blk_hash = T.hash
              WHERE 1=1 "
        where-stmt (if (nil? slot)
                     (str stmt (format "and epoch = %s" epoch))
                     (str stmt (format "and epoch = %s and slot = %s" epoch slot)))
        rows (query where-stmt)]
    (log/info "===query-epochs===:" where-stmt)
    rows))

(defn query-txs-hash
  [hash]
  (let [stmt (format "SELECT
                      'CTxBrief' as tag,
                      P.hash as ctb_id,
                      P.time as ctb_time_issued,
                      P.fees,
                      P.seal_inputs_address,
                      P.seal_inputs_amount,
                      P.seal_outputs_address,
                      P.seal_outputs_amount,
                      P.gold_inputs_address,
                      P.gold_inputs_amount,
                      P.gold_outputs_address,
                      P.gold_outputs_amount,
                      P.dollar_inputs_address,
                      P.dollar_inputs_amount,
                      P.dollar_outputs_address,
                      P.dollar_outputs_amount,
                      P.account_input_address,
                      P.account_output_address,
                      P.account_input_amt_seal,
                      P.account_input_amt_gold,
                      P.account_input_amt_dollar,
                      P.account_output_amt_seal,
                      P.account_output_amt_gold,
                      P.account_output_amt_dollar
                      FROM plain_txs P WHERE P.blk_hash = '%s'" hash)
        rows (query stmt)]
    (log/info "===query-txs-hash===:" stmt)
    rows))

(defn query-blocks-summary
  [hash]
  (let [stmt (format "SELECT
                      B.epoch AS cbe_epoch,
                      B.slot AS cbe_slot,
                      B.hash AS cbe_blk_hash,
                      B.time_issued AS cbe_time_issued,
                      B.tx_num AS cbe_tx_num,
                      B.size AS cbe_size,
                      B.block_leader AS cbe_block_lead,
                      P.fees,
                      P.seal_outputs_amount,
                      P.gold_outputs_amount,
                      P.dollar_outputs_amount,
                      P.account_output_amt_seal,
                      P.account_output_amt_gold,
                      P.account_output_amt_dollar,
                      B.prev_hash AS cbs_prev_hash,
                      NB.hash AS cbs_next_hash,
                      B.merkle_root AS cbs_merkle_root
                      FROM blocks B
                      LEFT JOIN blocks NB ON NB.prev_hash = B.hash
                      LEFT JOIN plain_txs P ON P.blk_hash = B.hash
                      WHERE B.hash = '%s'" hash)
        rows (query stmt)]
    (log/info "===query-blocks-summary===:" stmt)
    rows))

(defn query-txs-summary
  [txid]
  (let [stmt (format "SELECT 'CTxSummary' AS tag,
                      P.hash AS cts_id,
                      P.time AS cts_tx_time_issued,
                      B.time_issued AS cts_block_time_issued,
                      B.height AS cts_block_height,
                      B.epoch AS cts_block_epoch,
                      B.slot AS cts_block_slot,
                      B.hash AS cts_block_hash,
                      COALESCE(P.attachments, '') as cts_attachments,
                      P.fees,
                      P.seal_inputs_address,
                      P.seal_inputs_amount,
                      P.seal_outputs_address,
                      P.seal_outputs_amount,
                      P.gold_inputs_address,
                      P.gold_inputs_amount,
                      P.gold_outputs_address,
                      P.gold_outputs_amount,
                      P.dollar_inputs_address,
                      P.dollar_inputs_amount,
                      P.dollar_outputs_address,
                      P.dollar_outputs_amount,
                      P.account_input_address,
                      P.account_output_address,
                      P.account_input_amt_seal,
                      P.account_input_amt_gold,
                      P.account_input_amt_dollar,
                      P.account_output_amt_seal,
                      P.account_output_amt_gold,
                      P.account_output_amt_dollar,
                      P.cmd,
                      G.reason as gold_reason,
                      G.issued_golds,
                      D.reason as dollar_reason,
                      D.issued_dollars
                      FROM plain_txs P
                      LEFT JOIN blocks B ON P.blk_hash = B.hash
                      LEFT JOIN gold_issues G ON P.hash = G.hash
                      LEFT JOIN dollar_issues D ON P.hash = D.hash
                      WHERE P.hash = '%s'" txid)
        rows (query stmt)]
    (log/info "===query-txs-summary===:" stmt)
    rows))

(defn query-unlock-time
  [hash address]
  (let [stmt (format "SELECT unlock_time FROM utxos p where p.tx_hash = '%s' and p.receiver = Any(%s)" hash address)
        result (:unlock_time (query-one stmt))]
    (log/info "===query-unlock-time===:" stmt)
    result))

(defn query-issue-gold
  []
  (let [stmt "SELECT
                'CTxGoldIssueSummary' as tag,
                t.hash as cts_id,
                t.time as cts_tx_time_issued,
                b.time_issued as cts_block_time_issued,
                b.height as cts_block_height,
                b.epoch as cts_block_epoch,
                b.slot as cts_block_slot,
                b.hash as cts_block_hash,
                t.fees,
                g.issued_golds,
                g.destoryed_golds,
                g.seal_inputs_address,
                g.seal_inputs_amount,
                g.seal_outputs_address,
                g.seal_outputs_amount,
                g.gold_inputs_address,
                g.gold_inputs_amount,
                g.gold_outputs_address,
                g.gold_outputs_amount,
                g.reason as gold_reason
              FROM
                gold_issues g
              left join plain_txs t on t.hash = g.hash
              left join blocks b on b.hash = g.blk_hash"
        rows (query stmt)]
    (log/info "===query-issue-gold===:" stmt)
    rows))

(defn query-issue-dollar
  []
  (let [stmt "SELECT
                'CTxGoldIssueSummary' as tag,
                t.hash as cts_id,
                t.time as cts_tx_time_issued,
                b.time_issued as cts_block_time_issued,
                b.height as cts_block_height,
                b.epoch as cts_block_epoch,
                b.slot as cts_block_slot,
                b.hash as cts_block_hash,
                t.fees,
                g.issued_dollars,
                g.destoryed_dollars,
                g.seal_inputs_address,
                g.seal_inputs_amount,
                g.seal_outputs_address,
                g.seal_outputs_amount,
                g.gold_inputs_address,
                g.gold_inputs_amount,
                g.gold_outputs_address,
                g.gold_outputs_amount,
                g.dollar_inputs_address,
                g.dollar_inputs_amount,
                g.dollar_outputs_address,
                g.dollar_outputs_amount,
                g.reason as dollar_reason
              FROM
                dollar_issues g
              left join plain_txs t on t.hash = g.hash
              left join blocks b on b.hash = g.blk_hash"
        rows (query stmt)]
    (log/info "===query-issue-dollar===:" stmt)
    rows))

(defn query-addresses-summary-amt
  [address]
  (let [stmt (format "select (a_s.seal + u_s.seal) as seal_sum, (a_g.gold + u_g.gold) as gold_sum, (a_d.dollar + u_d.dollar) as dollar_sum from
                      (select COALESCE(sum(seal_balance), 0) as seal from accounts where account = any(array['%s'])) as a_s,
                      (select COALESCE(sum(amount), 0) as seal from utxos where receiver = any(array['%s']) and currency = 0) as u_s,
                      (select COALESCE(sum(gold_balance), 0) as gold from accounts where account = any(array['%s'])) as a_g,
                      (select COALESCE(sum(amount), 0) as gold from utxos where receiver = any(array['%s']) and currency = 1) as u_g,
                      (select COALESCE(sum(dollar_balance), 0) as dollar from accounts where account = any(array['%s'])) as a_d,
                      (select COALESCE(sum(amount), 0) as dollar from utxos where receiver = any(array['%s']) and currency = 2) as u_d"
                     address address
                     address address
                     address address)
        rows (query-one stmt)]
    (log/info "===query-addresses-summary-amt===:" stmt)
    rows))

(defn query-addresses-summary-tx
  [address]
  (let [stmt (format "SELECT
                        'CTxBrief' as tag,
                        P.hash AS cts_id,
                        P.time AS cts_tx_time_issued,
                        P.fees,
                        P.seal_inputs_address,
                        P.seal_inputs_amount,
                        P.seal_outputs_address,
                        P.seal_outputs_amount,
                        P.gold_inputs_address,
                        P.gold_inputs_amount,
                        P.gold_outputs_address,
                        P.gold_outputs_amount,
                        P.dollar_inputs_address,
                        P.dollar_inputs_amount,
                        P.dollar_outputs_address,
                        P.dollar_outputs_amount,
                        P.account_input_address,
                        P.account_output_address,
                        P.account_input_amt_seal,
                        P.account_input_amt_gold,
                        P.account_input_amt_dollar,
                        P.account_output_amt_seal,
                        P.account_output_amt_gold,
                        P.account_output_amt_dollar,
                        P.cmd
                        FROM plain_txs P
                        LEFT JOIN tx_addresses A ON A.tx_hash = P.hash
                        WHERE A.address = '%s'
                        ORDER BY P.time DESC" address)
        rows (query stmt)]
    (log/info "===query-addresses-summary-tx===:" stmt)
    rows))

(defn query-addresses-summary-token
  [address]
  (let [stmt (format "select COALESCE(sum(s.shares), 0) as share,
                      (select tm.type from token_meta tm where tm.id = s.token_id)
                      from token_holder s where s.account = '%s'
                      group by s.token_id" address)
        rows (query stmt)]
    (log/info "===query-addresses-summary-token===:" stmt)
    rows))

(defn query-token-list
  [page-size page-now]
  (let [stmt "SELECT
              t.id AS token_id,
              t.token AS token_name,
              t.total_shares AS total_supply,
              (t.total_shares - coalesce((sum(h.shares)),t.available_shares)) as supply,
              (select count(1) from token_holder s where s.token_id = t.id) AS holders,
              t.time AS create_time
              FROM token_meta t INNER JOIN token_holder h ON t.id = h.token_id AND h.is_owner = '1'
              GROUP BY t.id
              ORDER BY create_time DESC"
        page-stmt (build-page-query-row stmt page-size page-now)
        rows (query page-stmt)]
    (log/info "===query-token-list===:" page-stmt)
    rows))

(defn query-token-detail
  [token-id]
  (let [stmt (format "SELECT
              t.token AS token_name,
              t.total_shares AS total_supply,
              (t.total_shares - coalesce((sum(h.shares)),t.available_shares)) as supply,
              (select count(1) from token_holder s where s.token_id = t.id) AS holders,
              t.time AS create_time,
              (select count(1) from plain_txs where array_to_string(cmd, ' ') like '%%'||'send'||'%%'||t.id||'%%') as transactions,
              (select COALESCE((SUM(b.amount)::numeric / 10000), 0) as amount from token_bonus b where b.token_id = t.id) as total_profit
              FROM token_meta t INNER JOIN token_holder h ON t.id = h.token_id AND h.is_owner = '1'
              WHERE t.id = '%s'
              GROUP BY t.id" token-id)
        rows (query stmt)]
    (log/info "===query-token-detail===:" stmt)
    rows))

(defn query-token-issuance
  [token-id page-size page-now]
  (let [stmt (format "SELECT
                      T.tx_id AS tx_hash,
                      T.time,
                      T.account as owner,
                      COALESCE(T.issue_shares, 0) AS quantity
                      FROM token_issues T
                      WHERE T.token_id = '%s'
                      ORDER BY T.time DESC" token-id)
        page-stmt (build-page-query-page stmt page-size page-now)
        rows (query page-stmt)]
    (log/info "===query-token-issuance===:" page-stmt)
    rows))

(defn query-token-issuance-count
  [token-id]
  (let [stmt (format "SELECT
                      COUNT(1)
                      FROM token_issues T
                      WHERE T.token_id = '%s'" token-id)
        rows (query-one stmt)]
    (log/info "===query-token-issuance-count===:" stmt)
    rows))

(defn query-token-holders
  [token-id page-size page-now]
  (let [stmt (format "SELECT
                      S.account AS address,
                      COALESCE(S.shares, 0) AS quantity,
                      COALESCE(round((S.shares::numeric / (select sum(TI.issue_shares) from token_issues TI where TI.token_id = S.token_id)::numeric * 100), 3), 0) AS occupancy
                      FROM token_holder S
                      WHERE S.token_id = '%s'
                      ORDER BY quantity DESC" token-id)
        page-stmt (build-page-query-page stmt page-size page-now)
        rows (query page-stmt)]
    (log/info "===query-token-holders===:" page-stmt)
    rows))

(defn query-token-holders-count
  [token-id]
  (let [stmt (format "SELECT
                      COUNT(1)
                      FROM token_holder S
                      LEFT JOIN token_issues TI ON TI.token_id = S.token_id
                      WHERE S.token_id = '%s'" token-id)
        rows (query-one stmt)]
    (log/info "===query-token-holders-count===:" stmt)
    rows))

(defn query-token-profit
  [token-id page-size page-now]
  (let [stmt (format "SELECT
                      tr.tx_id as tx_hash,
                      tr.time,
                      tr.from_account as from_address,
                      COALESCE(tr.total_shares, 0) as share,
                      COALESCE(tr.total_bonus::numeric / 10000, 0) as profit,
                      COALESCE((pt.fees::numeric / 100000000), 0) as fees
                      FROM token_revenue tr
                      LEFT JOIN plain_txs pt on pt.hash = tr.tx_id
                      WHERE tr.token_id = '%s'
                      ORDER BY tr.time DESC" token-id)
        page-stmt (build-page-query-page stmt page-size page-now)
        rows (query page-stmt)]
    (log/info "===query-token-profit===:" page-stmt)
    rows))

(defn query-token-profit-count
  [token-id]
  (let [stmt (format "SELECT
                      COUNT(1)
                      FROM token_revenue tr
                      LEFT JOIN token_bonus tb on tb.revenue_id = tr.id
                      LEFT JOIN plain_txs pt on pt.hash = tr.tx_id
                      WHERE tr.token_id = '%s'" token-id)
        rows (query-one stmt)]
    (log/info "===query-token-profit-count===:" stmt)
    rows))

(defn owner?
  [token-id, addr]
  (let [stmt (format "SELECT COUNT(1) FROM token_holder S WHERE S.token_id = '%s' and S.account = '%s' AND is_owner = '1'" token-id addr)
        rows (query-one stmt)
        result (if (= 0 (:count rows)) false true)]
    result))

(defn issues-flag
  [tx-id]
  (let [gold-count (:count (query-one (format "SELECT COUNT(1) FROM gold_issues S WHERE S.hash = '%s'" tx-id)))
        dollar-count (:count (query-one (format "SELECT COUNT(1) FROM dollar_issues S WHERE S.hash = '%s'" tx-id)))
        result (cond
                 (> gold-count 0) "G"
                 (> dollar-count 0) "D"
                 :else "T")]
    result))