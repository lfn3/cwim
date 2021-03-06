(ns cwim.core
  (:require [org.httpkit.server :as hs]
            [org.httpkit.client :as hc]
            [clojure.core.async :as a]
            [clojure.edn :as edn]
            [clojure.spec :as s]
            [clojure.spec.gen :as gen]
            [clojure.string :as str]))

;TODO: implement me!
(defn validate-req
  "Check the msg is from the current epoch"
  ([srv msg]
    true))

(def default-port 64321)

(defn swap-return-atom! [atom f & xs]
  (apply swap! atom f xs)
  atom)

(defn read-internal-state [srv key]
  (-> srv
      ::internal-state
      (deref)
      key))

(defn update-internal-state [srv fn & args]
  (update srv ::internal-state swap-return-atom! #(apply fn %1 args)))

(defn add-to-send-queue [srv msg]
  (update-internal-state srv update ::send-queue conj (with-meta msg {::sent #{}})))

(defn add-node
  "Add another node to gossip to"
  ([srv host] (add-node srv host default-port))
  ([srv host port]
   (let [k {::host host ::port port}]
     (if-not (get (read-internal-state srv ::nodes) k)
       (-> srv
           (update-internal-state update ::nodes assoc k {::data {}
                                                          ::state :alive
                                                          ::clk 0
                                                          ::last-sent-epoch (read-internal-state srv ::epoch)})
           (update-internal-state update ::ordered-hosts conj k)
           (update-internal-state update ::ordered-hosts shuffle)
           (add-to-send-queue [::node-added {::host host
                                             ::port port}]))
       srv))))

(defn add-node-from-srv
  ([srv other-srv] (add-node srv (get-in other-srv [::cfg ::host]) (get-in other-srv [::cfg ::port]))))

(defmulti process-msg (fn [_ [key _]] key))

(defmethod process-msg ::node-added
  [{:keys [cwim.core/cfg] :as srv} [_ {:keys [cwim.core/host cwim.core/port] :as val}]]
  (when-not (= val (select-keys cfg [::host ::port]))         ;Ignore notes about ourselves
    (add-node srv host port)))

(defmethod process-msg :default
  [srv [key val]]
  (swap! (::state srv) assoc key val))

(defmulti handler (fn [msg srv] (::type msg)))

(defmethod handler :ping [{:keys [cwim.core/from] :as body} srv]
  (add-node srv (::host from) (::port from))
  (dorun (map (partial process-msg srv) (::messages body))))

(defn ping-handler [])

(defn indirect-ping-hander [srv])

(defn make-handler [srv]
  "Handler to recv messages from other nodes"
  ;TODO: this now needs to handle receiving acks as well.
  (fn [req]
    (when (validate-req srv (:body req))
      (let [body (edn/read-string (slurp (.bytes (:body req))))] ;TODO Change this. Perf is probably absymal.
        (handler body srv))))) ;TODO: send ack on ping

(defn move-elem-to-end-of-coll [coll elem]
  (-> coll
      ((partial remove #(= %1 elem)))
      (concat [elem])))

(defn send-impl [target msg]
  (let [target-str  (str "http://" (::host target) ":" (::port target))]
    (hc/post target-str
             {:body (pr-str msg)})))

(defn indirect-ping [node internal-state])

(defn dead [node internal-state]
  (swap! internal-state assoc-in [::nodes node ::state] :dead)
  (swap! internal-state update-in [::nodes node ::clk] inc)
  (swap! internal-state update ::ordered-hosts (partial remove #(= node %1))))

;TODO clean these up on shutdown of server.
(defn start-suspect-proc [node internal-state {:keys [cwim.core/ack-timeout cwim.core/suspect-timeout]}]
  (let [ack-ch (a/timeout ack-timeout)]
    (a/go
      (when-not (a/<! ack-ch)
        (let [suspect-clk (get-in @internal-state [::nodes node ::clk])]
          (swap! internal-state update-in [::nodes node] assoc ::state :suspect)
          (indirect-ping node @internal-state)
          (a/<! (a/timeout suspect-timeout))
          (when (= suspect-clk (get-in @internal-state [::nodes node ::clk]))
            (dead node internal-state)))))
    ack-ch))

(defn inc-or-remove-msg [vec msg target send-count]
  (let [idx (.indexOf vec msg)
        elem (get vec idx)
        updated-meta (update (meta elem) ::sent conj target)]
    (if (> send-count (count (::sent updated-meta)))
      (assoc vec idx (with-meta elem updated-meta))
      (concat (subvec vec 0 idx) (subvec vec (+ idx 1) (count vec))))))

(defn create-completion-fn [target msg internal-state {:keys [send-count] :as cfg}]
  (let [sus-ch (start-suspect-proc target internal-state cfg)]
    (fn []
      (a/put! sus-ch true)
      (swap! internal-state update
             ::send-queue
             inc-or-remove-msg
             msg
             target
             (max (count (::ordered-hosts internal-state)) send-count)))))

(defn make-message
  ([{:keys [cwim.core/internal-state cwim.core/cfg]}]
    (let [msgs-to-send (if-let [max (::max-msgs-per-epoch cfg)]
                         (take max (::send-queue @internal-state))
                         (::send-queue @internal-state))]
      {::messages msgs-to-send
       ::type      :ping
       ::from     (select-keys cfg [::host ::port])
       ::epoch    (inc (::epoch @internal-state))})))

(defn ping
  ([{:keys [cwim.core/internal-state cwim.core/cfg] :as srv}]
   (let [target-nodes (take (::gossip-send-count cfg) (::ordered-hosts @internal-state))
         msg (make-message srv)
         send-fn (:send-fn cfg)]
     (doseq [node target-nodes]
       (send-fn node msg)
       (swap! internal-state update ::ordered-hosts move-elem-to-end-of-coll node)
       (let [completion-fn (create-completion-fn node msg internal-state cfg)]
         (swap! internal-state update-in [::nodes node] assoc :completion-fn completion-fn)))
     (dorun (map #(send-fn %1 msg) target-nodes)))))

(def default-cfg {::host "127.0.0.1"
                  ::port default-port
                  ::node-data {}                    ;info about this node
                  ::max-msgs-per-epoch nil                   ;nil = unlimted
                  ::gossip-send-count 5
                  ::ping-timer 5000
                  ::suspect-timeout 10000
                  ::ack-timeout 2000
                  :send-fn send-impl             ;(fn [host msgs completion-fn]) -> nil
                  ::shutdown-fn (fn [srv] nil)        ;TODO move this around?
                  })

(defn srv-map [] {::cfg            default-cfg
                  ::internal-state (atom {::nodes {} ; {{:host "0.0.0.0" :port 1234} {:data {}
                                                                                  ; :state :alive or :suspect
                                                                                  ; :clk 0
                                                                                  ; :last-sent-epoch 123}}
                                          ::ordered-hosts '() ;{:host "0.0.0.0" :port 1234}
                                          ::send-queue []
                                          ::epoch 0})
                  ::state      (atom {})})

;; Public interface

(defn start
  "Starts a server to listen for gossip messages, and for holding cfg.
  Hang onto the return from this, you'll need it when discovering nodes or sending.
  Recommend you shove it in an atom (or mount/component)."
  ([] (start {}))
  ([cfg]
   (let [srv-map (assoc (srv-map) ::cfg (merge default-cfg cfg))
         srv (hs/run-server (make-handler srv-map) {:port (get-in srv-map [::cfg ::port])})
         ping-time (get-in srv-map [::cfg ::ping-timer])
         control-ch (a/chan)]
     (a/go-loop []
       (let [[v ch] (a/alts! [(a/timeout ping-time)
                              control-ch])]
         (if (= ch control-ch)
           (srv)
           (do
             (try
               (ping srv-map)
               (catch Exception e
                 ;TODO Error handling strategy
                 (prn e)))
             (recur)))))
     ;TODO shoving this in the cfg map doesn't seem right, precludes using a different server?
     ;TODO put this somwhere else, the shutdown-fn should be produced by the server-start-fn,
     ;TODO and invoked inside the go-loop above.
     (assoc-in srv-map [::cfg ::shutdown-fn] (fn [_] (a/put! control-ch :shutdown))))))

(s/fdef start
  :args (s/cat :cfg ::cfg)
  :ret [::srv-map])

(defn stop
  "Stops a running server"
  [srv]
  ((get-in srv [::cfg ::shutdown-fn] (fn [_]
                                       (prn "Shutdown not defined, doing nothing...")
                                       nil)) srv))

(defn get-epoch [srv]
  (-> srv
      ::internal-state
      deref
      ::epoch))

(defn send-msg
  ([srv key val]
   (-> srv
       (add-to-send-queue [key val])
       ::state
       (swap! assoc key val))
    ;All the operations here operate on the atoms, so this should be fine.
    srv))

(defn nodes
  [srv]
  (->> srv
       ::internal-state
       (deref)
       ::nodes
       (filter #(not= :dead (::state (val %1))))))

;TODO: replace this by making srv return state when derefed.
(defn query
  ([srv key]
   (-> srv
       ::state
       (deref)
       (get key))))

(s/def ::host (s/with-gen (s/and string?
                                 #(>= (count "111.111.111.111") (count %1))
                                 #(re-matches #"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}" %1))
                          #(gen/fmap
                            (partial str/join ".")
                            (apply gen/tuple (repeatedly 4 (partial gen/choose 1 255))))))
(s/def ::port pos-int?)

(s/def ::other-node-data associative?)
(s/def ::node-state #{:alive :suspect :dead})
(s/def ::clk int?)
(s/def ::last-sent-epoch int?)

(s/def ::host-key-map (s/keys :req [::host ::port]))
(s/def ::from ::host-key-map)
(s/def ::type #{:ping :indirect-ping})
(s/def ::messages seq?)

(s/def ::other-nodes (s/map-of ::host-key-map
                               (s/keys :req [::other-node-data ::node-state ::clk ::last-sent-epoch])))

(s/def ::ordered-hosts (s/coll-of ::host-key-map))
(s/def ::internal-state (s/keys :req [::other-nodes ::ordered-hosts ::send-queue ::epoch]))
(s/def ::state associative?)

(s/def ::max-msgs-per-epoch pos-int?)
(s/def ::gossip-send-count pos-int?)
(s/def ::ping-timer (s/and pos-int? #(>= % 1000)))
(s/def ::suspect-timeout (s/and pos-int? #(>= % 1000)))
(s/def ::ack-timeout (s/and pos-int? #(>= % 1000)))
(s/def ::send-fn (s/with-gen fn?
                             #(gen/return send-impl)))
(s/def ::shutdown-fn (s/with-gen fn?
                                 #(gen/return (fn []))))

(s/def ::cfg (s/keys :req [::host
                           ::port
                           ::max-msgs-per-epoch
                           ::gossip-send-count
                           ::ping-timer
                           ::suspect-timeout
                           ::ack-timeout
                           ::send-fn
                           ::shutdown-fn]))

(s/def ::srv-map (s/keys :req [::cfg ::internal-state ::state ::shutdown]))
