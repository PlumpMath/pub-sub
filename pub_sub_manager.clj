(ns cinv.core.queue
  (:require [mount.core :as mount]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async]  ))

(mount/defstate ^{:on-reload :noop}
                channel-manager
                :start (atom {})
                :stop #((prn "Channel manager state: " @channel-manager)
                        (atom {})))


(mount/defstate ^{:on-reload :noop}
                channel-config
                :start [:test-ns :example :user :another-ns]
                :stop  nil))

(defn open-managed-channel
  "Opens channel and assocs to atom with given key to allow closing when needed,
  and returns said channel."
  [k atom]
  (let [ch (async/chan)]
    (swap! atom assoc k ch)
    ch))


(defn thread-checker [payload] (prn payload))

(defn subscribe-worker
  "Creates a subscription channel which listens to given emitter for given subscription type, and applies
  given func when receiving message. Given func takes :payload key from emitter and subscription type
  as arguments"
  [emitter sub-type func manager]
  (let [ch (open-managed-channel sub-type manager)]
    (async/sub emitter sub-type ch)
    (log/info "Successfully subscribed: " sub-type)
    (async/go-loop []
      (let [{:keys [payload]} (async/<! ch)]
        (func sub-type payload)
        (recur)))))

(defn initialize-subscribers
  ([coll emitter manager]
   (doseq [worker coll]
     (subscribe-worker emitter worker thread-checker manager))))

(defn close-managed-channels
  ([] close-managed-channels channel-manager)
  ([manager]
   (prn "Closing managed channels.")
   (doseq [k (keys @manager)]
     (async/close! (k @manager))
     (swap! manager dissoc k))))


(defn open-publication-channel
  "Hot mess of state. Opens publication channel with :look-up-key flag.

  Initializes subscribers to listen from config list (e.g. [:user :case] will
  yield listening channels which are listening for {:ent :user} and {:ent :case}
  respectively. See subscribe-worker for processing details.

  All async channels are swapped into channel manager. Returns
  hash with :stop method for shutting down managed channels on invocation."
  ([] (open-publication-channel :pub channel-manager))
  ([look-up-key manager]
   (let [pub-chan (open-managed-channel look-up-key manager)
         publication (async/pub pub-chan :ent)]
     (initialize-subscribers channel-config publication manager)
     {:publish (partial async/put! pub-chan)
      :stop    #(close-managed-channels)})))

(mount/defstate ^{:on-reload :noop}
                queueing-engine
                :start (open-publication-channel)
                :stop ((:stop queueing-engine)))