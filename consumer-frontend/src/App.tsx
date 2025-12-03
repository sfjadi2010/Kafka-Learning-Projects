import { useState, useEffect } from 'react';
import axios from 'axios';
import { Record, Stats, ConsumerHealth } from './types';
import './App.css';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8001';

function App() {
  const [records, setRecords] = useState<Record[]>([]);
  const [stats, setStats] = useState<Stats | null>(null);
  const [health, setHealth] = useState<ConsumerHealth | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [limit, setLimit] = useState(50);
  const [offset, setOffset] = useState(0);
  const [totalRecords, setTotalRecords] = useState(0);
  const [consumerRunning, setConsumerRunning] = useState(false);

  useEffect(() => {
    checkHealth();
    fetchStats();
    fetchRecords();
    const interval = setInterval(() => {
      fetchStats();
      if (consumerRunning) {
        fetchRecords();
      }
    }, 5000);
    return () => clearInterval(interval);
  }, [offset, limit, consumerRunning]);

  const checkHealth = async () => {
    try {
      const response = await axios.get(`${API_URL}/health`);
      setHealth(response.data);
    } catch (err) {
      console.error('Health check failed:', err);
    }
  };

  const fetchStats = async () => {
    try {
      const response = await axios.get(`${API_URL}/stats`);
      setStats(response.data);
      setConsumerRunning(response.data.consumer_status === 'running');
    } catch (err) {
      console.error('Failed to fetch stats:', err);
    }
  };

  const fetchRecords = async () => {
    setLoading(true);
    setError('');
    try {
      const countResponse = await axios.get(`${API_URL}/records/count`);
      setTotalRecords(countResponse.data.count);

      const response = await axios.get(`${API_URL}/records`, {
        params: { limit, offset }
      });
      setRecords(response.data);
    } catch (err) {
      setError('Failed to fetch records');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const startConsumer = async () => {
    try {
      await axios.post(`${API_URL}/start-consumer`);
      setConsumerRunning(true);
      setTimeout(fetchStats, 1000);
    } catch (err: unknown) {
      if (axios.isAxiosError(err)) {
        setError(err.response?.data?.detail || 'Failed to start consumer');
      }
    }
  };

  const stopConsumer = async () => {
    try {
      await axios.post(`${API_URL}/stop-consumer`);
      setConsumerRunning(false);
      setTimeout(fetchStats, 1000);
    } catch (err: unknown) {
      if (axios.isAxiosError(err)) {
        setError(err.response?.data?.detail || 'Failed to stop consumer');
      }
    }
  };

  const deleteAllRecords = async () => {
    if (window.confirm('Are you sure you want to delete all records?')) {
      try {
        await axios.delete(`${API_URL}/records`);
        setRecords([]);
        setTotalRecords(0);
        fetchStats();
      } catch (err: unknown) {
        if (axios.isAxiosError(err)) {
          setError(err.response?.data?.detail || 'Failed to delete records');
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
    if (records.length === 0) return [];
    return Object.keys(records[0]);
  };

  return (
    <div className="app">
      <header className="header">
        <h1>📊 Kafka Consumer - Data Viewer</h1>
        <div className="health-status">
          {health && (
            <span className={`status-badge ${health.status === 'healthy' ? 'healthy' : 'unhealthy'}`}>
              {health.status === 'healthy' ? '✓' : '✗'} Database: {health.database}
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
          <button 
            onClick={deleteAllRecords} 
            className="btn btn-danger"
          >
            🗑️ Delete All
          </button>
          <button 
            onClick={fetchRecords} 
            className="btn btn-secondary"
          >
            🔄 Refresh
          </button>
        </div>

        {stats && (
          <div className="stats-panel">
            <div className="stat-item">
              <span className="stat-label">Total Records:</span>
              <span className="stat-value">{stats.total_records}</span>
            </div>
            <div className="stat-item">
              <span className="stat-label">Consumer Status:</span>
              <span className={`stat-value status-${stats.consumer_status}`}>
                {stats.consumer_status}
              </span>
            </div>
            <div className="stat-item">
              <span className="stat-label">Last Updated:</span>
              <span className="stat-value">{new Date(stats.last_updated).toLocaleString()}</span>
            </div>
          </div>
        )}
      </div>

      {error && (
        <div className="error-message">
          ⚠️ {error}
        </div>
      )}

      <div className="table-container">
        {loading ? (
          <div className="loading">Loading...</div>
        ) : records.length === 0 ? (
          <div className="no-data">
            <p>No records found</p>
            <p>Start the consumer to begin processing Kafka messages</p>
          </div>
        ) : (
          <>
            <table className="data-table">
              <thead>
                <tr>
                  {getColumns().map(col => (
                    <th key={col}>{col}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {records.map((record, idx) => (
                  <tr key={idx}>
                    {getColumns().map(col => (
                      <td key={col}>{String(record[col])}</td>
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
                Showing {offset + 1} - {Math.min(offset + limit, totalRecords)} of {totalRecords}
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
    </div>
  );
}

export default App;
