(ns bpmn.execute
  "A pure, token-based interpreter for a BPMN-as-EDN model. State is plain data so a
  run is fully inspectable, replayable and testable offline with fixture ports.

  Tokens are advanced one at a time (`advance`); `run` folds `advance` to quiescence.
  A token is `{:bpmn/at node-id :bpmn/via flow-id}` (the flow it arrived on, so an
  AND-join can tell which incoming branches have delivered). Process variables live
  in one global `:bpmn/vars` map, mutated only by `IActivity/perform`.

  Gateway semantics:
    exclusive  — first outgoing whose condition is truthy, else the `:bpmn/default`
                 flow, else the first unconditioned flow (XOR-join just passes each
                 token through its single outgoing).
    parallel   — AND-join + AND-split: wait until a token has arrived on *every*
                 incoming flow, then emit one on *every* outgoing.
    inclusive  — OR-split: emit on every outgoing whose condition is truthy (or the
                 default if none). NOTE: OR-*join* is treated as pass-through.
  A non-gateway node performs its activity then fans a token onto each outgoing flow
  (0 → the token ends, 1 → moves, >1 → uncontrolled parallel split). An end event
  performs then consumes its token."
  (:require [clojure.string :as str]
            [bpmn.model :as m]
            [bpmn.ports :as p]))

(defn start
  "Initial run state: one token at each start event."
  ([model] (start model {}))
  ([model vars]
   {:bpmn/vars (or vars {})
    :bpmn/tokens (mapv (fn [se] {:bpmn/at (:bpmn/id se) :bpmn/via nil})
                       (m/start-events model))
    :bpmn/arrived {}                       ; gateway-id → #{arrived-flow-ids}
    :bpmn/trace []
    :bpmn/done? false}))

(defn completed? [state]
  (and (:bpmn/done? state) (empty? (:bpmn/tokens state))))

(defn- emit [outs] (mapv (fn [f] {:bpmn/at (:bpmn/target f) :bpmn/via (:bpmn/id f)}) outs))

(defn advance
  "Advance exactly one token by one node. Returns the next state."
  [ports model state]
  (let [tokens (:bpmn/tokens state)]
    (if (empty? tokens)
      (assoc state :bpmn/done? true)
      (let [tok    (nth tokens 0)
            others (subvec tokens 1)
            nid    (:bpmn/at tok)
            node   (m/node model nid)
            t      (:bpmn/type node)
            outs   (m/outgoing model nid)
            vars   (:bpmn/vars state)
            log    (fn [st ev] (update st :bpmn/trace conj (assoc ev :bpmn/at nid)))]
        (cond
          (= t :end-event)
          (-> state
              (assoc :bpmn/tokens others
                     :bpmn/vars (p/perform (:activity ports) node vars))
              (log {:bpmn/event :end}))

          (= t :parallel-gateway)
          (let [ins      (m/incoming model nid)
                arrived' (conj (get-in state [:bpmn/arrived nid] #{}) (:bpmn/via tok))]
            (if (>= (count arrived') (max 1 (count ins)))
              (-> state
                  (assoc :bpmn/tokens (into others (emit outs)))
                  (assoc-in [:bpmn/arrived nid] #{})
                  (log {:bpmn/event :parallel-fire}))
              (-> state
                  (assoc :bpmn/tokens others)
                  (assoc-in [:bpmn/arrived nid] arrived')
                  (log {:bpmn/event :parallel-wait}))))

          (= t :exclusive-gateway)
          (let [chosen (or (first (filter #(and (:bpmn/condition %)
                                                (p/truthy? (:condition ports)
                                                           (:bpmn/condition %) vars))
                                          outs))
                           (some->> (:bpmn/default node) (m/flow model))
                           (first (remove :bpmn/condition outs))
                           (first outs))]
            (-> state
                (assoc :bpmn/tokens (into others (emit (when chosen [chosen]))))
                (log {:bpmn/event :exclusive :bpmn/flow (:bpmn/id chosen)})))

          (= t :inclusive-gateway)
          (let [taken (filter #(or (nil? (:bpmn/condition %))
                                   (p/truthy? (:condition ports) (:bpmn/condition %) vars))
                              outs)
                taken (cond
                        (seq taken) taken
                        (:bpmn/default node) (some->> (:bpmn/default node) (m/flow model) vector)
                        ;; No condition was truthy and there's no default flow --
                        ;; falling through to an empty `taken` here would silently
                        ;; annihilate the token (no error, no end event, and `run`
                        ;; would report the process as completed?). Fall back to
                        ;; every outgoing flow rather than dropping the token, the
                        ;; same never-lose-a-token guarantee exclusive-gateway's
                        ;; own final fallback provides.
                        :else outs)]
            (-> state
                (assoc :bpmn/tokens (into others (emit taken)))
                (log {:bpmn/event :inclusive})))

          :else                                    ; start / activity / event
          (-> state
              (assoc :bpmn/vars (p/perform (:activity ports) node vars)
                     :bpmn/tokens (into others (emit outs)))
              (log {:bpmn/event (if (seq outs) :pass :implicit-end)})))))))

(defn run
  "Fold `advance` until no tokens remain (or `max-steps`, a runaway guard, is hit;
  then `:bpmn/error :step-limit` is set)."
  ([ports model] (run ports model (start model) 10000))
  ([ports model state] (run ports model state 10000))
  ([ports model state max-steps]
   (loop [st state n 0]
     (cond
       (empty? (:bpmn/tokens st)) (assoc st :bpmn/done? true)
       (>= n max-steps)           (assoc st :bpmn/error :step-limit :bpmn/done? false)
       :else                      (recur (advance ports model st) (inc n))))))

;; --- a host-free default so any model is runnable out of the box ---

(defn default-ports
  "Activities are no-ops (vars unchanged); a condition is read as the boolean process
  variable it names — `${approved}`, `approved`, `= approved` all look up
  `(:approved vars)`. Enough to exercise control flow; replace for real work."
  []
  {:activity  (reify p/IActivity (perform [_ _ vars] vars))
   :condition (reify p/ICondition
                (truthy? [_ condition vars]
                  (let [k (-> (str condition) (str/replace #"[${}=\s]" ""))]
                    (boolean (get vars (keyword k))))))})
