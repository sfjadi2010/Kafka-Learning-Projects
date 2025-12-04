import { useState, useEffect } from "react";
import axios from "axios";
import { Record, Topic, Stats, ConsumerHealth } from "./types";
import "./App.css";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8001";

function App() {
  const [topics, setTopics] = useState<Topic[]>([]);
  const [activeTab, setActiveTab] = useState<string | null>(null);
  const [records, setRecords] = useState<Record[]>([]);
  const [stats, setStats] = useState<Stats | null>(null);
  const [health, setHealth] = useState<ConsumerHealth | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [limit, setLimit] = useState(50);
  const [offset, setOffset] = useState(0);
  const [totalRecords, setTotalRecords] = useState(0);
  const [consumerRunning, setConsumerRunning] = useState(false);
  const [searchTerm, setSearchTerm] = useState("");
  const [selectedRecord, setSelectedRecord] = useState<Record | null>(null);
  const [editedData, setEditedData] = useState<any>({});
  const [isModalOpen, setIsModalOpen] = useState(false);

  useEffect(() => {
    checkHealth();
    fetchStats();
    fetchTopics();
  }, []);

  useEffect(() => {
    if (activeTab) {
      fetchTopicRecords(activeTab);
    }
  }, [activeTab, offset, limit]);

  useEffect(() => {
    const interval = setInterval(() => {
      fetchStats();
      fetchTopics();
      if (consumerRunning && activeTab) {
        fetchTopicRecords(activeTab);
      }
    }, 5000);
    return () => clearInterval(interval);
  }, [consumerRunning, activeTab, offset, limit]);

  const checkHealth = async () => {
    try {
      const response = await axios.get(`${API_URL}/health`);
      setHealth(response.data);
    } catch (err) {
      console.error("Health check failed:", err);
    }
  };

  const fetchStats = async () => {
    try {
      const response = await axios.get(`${API_URL}/stats`);
      setStats(response.data);
      setConsumerRunning(response.data.current_consumer_active);
    } catch (err) {
      console.error("Failed to fetch stats:", err);
    }
  };

  const fetchTopics = async () => {
    try {
      const response = await axios.get(`${API_URL}/topics`);
      console.log("Raw API response:", response.data);
      const fetchedTopics = response.data.topics || [];
      console.log("Fetched topics:", fetchedTopics);
      console.log("Topics count:", fetchedTopics.length);
      setTopics(fetchedTopics);

      // Set active tab to first topic if not already set
      if (fetchedTopics.length > 0 && !activeTab) {
        setActiveTab(fetchedTopics[0].topic_name);
      }
    } catch (err) {
      console.error("Failed to fetch topics:", err);
    }
  };

  const fetchTopicRecords = async (topic: string) => {
    setLoading(true);
    setError("");
    try {
      const response = await axios.get(`${API_URL}/topics/${topic}/records`, {
        params: { limit, offset },
      });
      setRecords(response.data.records || []);
      setTotalRecords(response.data.total || 0);
    } catch (err) {
      setError("Failed to fetch records");
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const startConsumer = async () => {
    setError("");
    try {
      const response = await axios.post(`${API_URL}/start-consumer`);
      if (response.data.status === "already_running") {
        setError("Consumer is already running");
      }
      setConsumerRunning(true);
      setTimeout(fetchStats, 1000);
    } catch (err: unknown) {
      if (axios.isAxiosError(err)) {
        setError(
          err.response?.data?.detail ||
            err.response?.data?.message ||
            "Failed to start consumer"
        );
      }
    }
  };

  const stopConsumer = async () => {
    setError("");
    try {
      const response = await axios.post(`${API_URL}/stop-consumer`);
      if (response.data.status === "not_running") {
        setError("Consumer is not running");
      }
      setConsumerRunning(false);
      setTimeout(fetchStats, 1000);
    } catch (err: unknown) {
      if (axios.isAxiosError(err)) {
        setError(err.response?.data?.detail || "Failed to stop consumer");
      }
    }
  };

  const deleteAllRecords = async () => {
    if (
      window.confirm(
        "Are you sure you want to delete all records? This will archive and delete all topics."
      )
    ) {
      try {
        await axios.delete(`${API_URL}/records`);
        setRecords([]);
        setTotalRecords(0);
        setTopics([]);
        setActiveTab(null);
        fetchStats();
        fetchTopics();
      } catch (err: unknown) {
        if (axios.isAxiosError(err)) {
          setError(err.response?.data?.detail || "Failed to delete records");
        }
      }
    }
  };

  const deleteTopic = async (topic: string) => {
    if (
      window.confirm(
        `Are you sure you want to delete topic "${topic}"? This will archive and delete all data for this topic.`
      )
    ) {
      try {
        await axios.delete(`${API_URL}/topics/${topic}`);

        // Remove topic from state
        const updatedTopics = topics.filter((t) => t.topic_name !== topic);
        setTopics(updatedTopics);

        // If deleted topic was active, switch to another tab or clear
        if (activeTab === topic) {
          if (updatedTopics.length > 0) {
            setActiveTab(updatedTopics[0].topic_name);
          } else {
            setActiveTab(null);
            setRecords([]);
            setTotalRecords(0);
          }
        }

        fetchStats();
        fetchTopics();
      } catch (err: unknown) {
        if (axios.isAxiosError(err)) {
          setError(err.response?.data?.detail || "Failed to delete topic");
        }
      }
    }
  };

  const nextPage = () => {
    if (offset + limit < totalRecords) {
      setOffset(offset + limit);
    }
  };

  const prevPage = () => {
    if (offset >= limit) {
      setOffset(offset - limit);
    }
  };

  const getColumns = () => {
    if (records.length === 0 || !records[0] || !records[0].data) return [];
    return Object.keys(records[0].data);
  };

  const isRowMatch = (record: Record) => {
    if (!searchTerm.trim()) return false;
    const searchLower = searchTerm.toLowerCase();
    return getColumns().some((col) =>
      String(record.data[col] || "")
        .toLowerCase()
        .includes(searchLower)
    );
  };

  const openModal = (record: Record) => {
    setSelectedRecord(record);
    setEditedData({ ...record.data });
    setIsModalOpen(true);
  };

  const closeModal = () => {
    setIsModalOpen(false);
    setSelectedRecord(null);
    setEditedData({});
  };

  const handleFieldChange = (field: string, value: string) => {
    setEditedData({ ...editedData, [field]: value });
  };

  const saveRecord = async () => {
    if (!selectedRecord || !activeTab) return;

    try {
      // TODO: Implement API endpoint to update record
      // For now, just update locally and close modal
      console.log("Saving record:", editedData);
      
      // Update the record in the local state
      const updatedRecords = records.map((rec) =>
        rec.id === selectedRecord.id ? { ...rec, data: editedData } : rec
      );
      setRecords(updatedRecords);
      
      closeModal();
      alert("Record updated successfully!");
    } catch (err) {
      console.error("Failed to save record:", err);
      alert("Failed to save record");
    }
  };

  return (
    <div className="app">
      <header className="header">
        <h1>📊 Kafka Consumer - Data Viewer</h1>
        <div className="health-status">
          {health && (
            <span
              className={`status-badge ${
                health.status === "healthy" ? "healthy" : "unhealthy"
              }`}
            >
              {health.status === "healthy" ? "✓" : "✗"} Database:{" "}
              {health.database}
            </span>
          )}
        </div>
      </header>

      <div className="controls">
        <div className="control-group">
          <button
            onClick={startConsumer}
            disabled={consumerRunning}
            className="btn btn-primary"
          >
            ▶️ Start Consumer
          </button>
          <button
            onClick={stopConsumer}
            disabled={!consumerRunning}
            className="btn btn-warning"
          >
            ⏸️ Stop Consumer
          </button>
          <button onClick={deleteAllRecords} className="btn btn-danger">
            🗑️ Delete All
          </button>
          <button
            onClick={() => activeTab && fetchTopicRecords(activeTab)}
            className="btn btn-secondary"
          >
            🔄 Refresh
          </button>
        </div>

        {stats && (
          <div className="stats-panel">
            <div className="stat-item">
              <span className="stat-label">Total Records:</span>
              <span className="stat-value">{totalRecords}</span>
            </div>
            <div className="stat-item">
              <span className="stat-label">Consumer Status:</span>
              <span className={`stat-value status-${stats.status}`}>
                {stats.status}
              </span>
            </div>
            <div className="stat-item">
              <span className="stat-label">Last Updated:</span>
              <span className="stat-value">
                {stats.last_consumed_at
                  ? new Date(stats.last_consumed_at + "Z").toLocaleString()
                  : "N/A"}
              </span>
            </div>
          </div>
        )}
      </div>

      {error && <div className="error-message">⚠️ {error}</div>}

      {topics.length > 0 && (
        <div className="tabs-container">
          <div className="tabs">
            {topics.map((topic) => (
              <div
                key={topic.topic_name}
                className={`tab-wrapper ${
                  activeTab === topic.topic_name ? "active" : ""
                }`}
              >
                <button
                  className={`tab ${
                    activeTab === topic.topic_name ? "active" : ""
                  }`}
                  onClick={() => {
                    setActiveTab(topic.topic_name);
                    setOffset(0);
                  }}
                >
                  {topic.topic_name}
                  <span className="badge">{topic.record_count}</span>
                </button>
                <button
                  className="tab-delete"
                  onClick={(e) => {
                    e.stopPropagation();
                    deleteTopic(topic.topic_name);
                  }}
                  title={`Delete ${topic.topic_name}`}
                >
                  ✕
                </button>
              </div>
            ))}
          </div>
          <div className="search-box">
            <input
              type="text"
              placeholder="🔍 Search in current tab..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="search-input"
            />
            {searchTerm && (
              <button
                onClick={() => setSearchTerm("")}
                className="search-clear"
                title="Clear search"
              >
                ✕
              </button>
            )}
          </div>
        </div>
      )}

      <div className="table-container">
        {loading ? (
          <div className="loading">Loading...</div>
        ) : topics.length === 0 ? (
          <div className="no-data">
            <p>No topics found</p>
            <p>Upload a CSV file to create topics and start processing</p>
          </div>
        ) : records.length === 0 ? (
          <div className="no-data">
            <p>No records found in {activeTab}</p>
          </div>
        ) : (
          <>
            <table className="data-table">
              <thead>
                <tr>
                  {getColumns().map((col) => (
                    <th key={col}>{col}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {records &&
                  records.map((record, idx) => (
                    <tr
                      key={idx}
                      className={isRowMatch(record) ? "highlight" : ""}
                      onClick={() => openModal(record)}
                      style={{ cursor: "pointer" }}
                    >
                      {getColumns().map((col) => (
                        <td key={col}>{String(record.data[col] || "")}</td>
                      ))}
                    </tr>
                  ))}
              </tbody>
            </table>

            <div className="pagination">
              <button
                onClick={prevPage}
                disabled={offset === 0}
                className="btn btn-secondary"
              >
                ← Previous
              </button>
              <span className="pagination-info">
                Showing {offset + 1} - {Math.min(offset + limit, totalRecords)}{" "}
                of {totalRecords}
              </span>
              <button
                onClick={nextPage}
                disabled={offset + limit >= totalRecords}
                className="btn btn-secondary"
              >
                Next →
              </button>
              <select
                value={limit}
                onChange={(e) => {
                  setLimit(Number(e.target.value));
                  setOffset(0);
                }}
                className="limit-select"
                aria-label="Records per page"
              >
                <option value="10">10 per page</option>
                <option value="25">25 per page</option>
                <option value="50">50 per page</option>
                <option value="100">100 per page</option>
              </select>
            </div>
          </>
        )}
      </div>

      {/* Modal Popup */}
      {isModalOpen && selectedRecord && (
        <div className="modal-overlay" onClick={closeModal}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h2>{activeTab}</h2>
              <button className="modal-close" onClick={closeModal}>
                ✕
              </button>
            </div>
            <div className="modal-body">
              <div className="modal-fields">
                {Object.keys(editedData).map((field) => (
                  <div key={field} className="modal-field">
                    <label className="modal-label">{field}:</label>
                    <input
                      type="text"
                      className="modal-input"
                      value={editedData[field] || ""}
                      onChange={(e) => handleFieldChange(field, e.target.value)}
                    />
                  </div>
                ))}
              </div>
            </div>
            <div className="modal-footer">
              <button className="btn btn-secondary" onClick={closeModal}>
                Cancel
              </button>
              <button className="btn btn-primary" onClick={saveRecord}>
                Save Changes
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default App;
