(ns bpmn.execute-test
  (:require [clojure.test :refer [deftest is testing]]
            [bpmn.model :as m]
            [bpmn.ports :as p]
            [bpmn.execute :as e]))

;; --- exclusive gateway: condition routes the single token ---

(defn approval-model []
  (-> (m/process "Order")
      (m/add :start-event "S")
      (m/add :exclusive-gateway "G" {:default "Freject"})
      (m/add :end-event "Eok") (m/add :end-event "Eno")
      (m/connect "S" "G")
      (m/connect "G" "Eok" {:id "Fapprove" :condition "${approved}"})
      (m/connect "G" "Eno" {:id "Freject"})))

(deftest exclusive-routes-on-condition
  (let [model (approval-model)]
    (testing "approved → the conditioned branch"
      (let [end (e/run (e/default-ports) model (e/start model {:approved true}))]
        (is (e/completed? end))
        (is (some #(= "Eok" (:bpmn/at %))
                  (filter #(= :end (:bpmn/event %)) (:bpmn/trace end))))))
    (testing "not approved → the default branch"
      (let [end (e/run (e/default-ports) model (e/start model {:approved false}))]
        (is (some #(= "Eno" (:bpmn/at %))
                  (filter #(= :end (:bpmn/event %)) (:bpmn/trace end))))))))

;; --- parallel gateway: AND-split then AND-join, with a side-effecting activity ---

(defrecord RecordingActivity [seen]
  p/IActivity
  (perform [_ node vars]
    (swap! seen conj (:bpmn/id node))
    (cond-> vars (= :service-task (:bpmn/type node))
            (update :visited (fnil conj #{}) (:bpmn/id node)))))

(defn fork-join-model []
  (-> (m/process "Fan")
      (m/add :start-event "S")
      (m/add :parallel-gateway "Split")
      (m/add :service-task "A") (m/add :service-task "B")
      (m/add :parallel-gateway "Join")
      (m/add :end-event "E")
      (m/connect "S" "Split")
      (m/connect "Split" "A") (m/connect "Split" "B")
      (m/connect "A" "Join") (m/connect "B" "Join")
      (m/connect "Join" "E")))

(deftest parallel-forks-and-joins-once
  (let [model (fork-join-model)
        seen  (atom [])
        ports {:activity (->RecordingActivity seen)
               :condition (reify p/ICondition (truthy? [_ _ _] true))}
        end   (e/run ports model)]
    (testing "both branches ran"
      (is (e/completed? end))
      (is (= #{"A" "B"} (:visited (:bpmn/vars end)))))
    (testing "the join fires exactly once (one end event)"
      (is (= 1 (count (filter #(= :end (:bpmn/event %)) (:bpmn/trace end))))))
    (testing "the join waited before firing"
      (is (some #(= :parallel-wait (:bpmn/event %)) (:bpmn/trace end)))
      (is (some #(= :parallel-fire (:bpmn/event %)) (:bpmn/trace end))))))

;; --- runaway guard ---

(deftest step-limit-guards-loops
  (let [model (-> (m/process "Loop")
                  (m/add :start-event "S") (m/add :task "T")
                  (m/connect "S" "T") (m/connect "T" "T"))   ; self-loop, never ends
        end (e/run (e/default-ports) model (e/start model) 50)]
    (is (= :step-limit (:bpmn/error end)))))
