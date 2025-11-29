import { useState, useEffect } from "react";
import axios from "axios";
import type { UploadResult, KafkaHealth } from "./types";
import "./App.css";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

function App() {
  const [file, setFile] = useState<File | null>(null);
  const [uploading, setUploading] = useState(false);
  const [uploadResult, setUploadResult] = useState<UploadResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [dragActive, setDragActive] = useState(false);
  const [kafkaHealth, setKafkaHealth] = useState<KafkaHealth | null>(null);

  useEffect(() => {
    checkHealth();
  }, []);

  const checkHealth = async () => {
    try {
      const response = await axios.get<KafkaHealth>(`${API_URL}/health`);
      setKafkaHealth(response.data);
    } catch (err) {
      console.error("Health check failed:", err);
      setKafkaHealth({ status: "unhealthy", error: (err as Error).message });
    }
  };

  const handleDrag = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") {
      setDragActive(true);
    } else if (e.type === "dragleave") {
      setDragActive(false);
    }
  };

  const handleDrop = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);

    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      const droppedFile = e.dataTransfer.files[0];
      if (droppedFile.name.endsWith(".csv")) {
        setFile(droppedFile);
        setError(null);
      } else {
        setError("Please upload a CSV file");
      }
    }
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      const selectedFile = e.target.files[0];
      if (selectedFile.name.endsWith(".csv")) {
        setFile(selectedFile);
        setError(null);
      } else {
        setError("Please upload a CSV file");
        setFile(null);
      }
    }
  };

  const handleUpload = async () => {
    if (!file) {
      setError("Please select a file first");
      return;
    }

    setUploading(true);
    setError(null);
    setUploadResult(null);

    const formData = new FormData();
    formData.append("file", file);

    try {
      const response = await axios.post<UploadResult>(
        `${API_URL}/upload-csv`,
        formData,
        {
          headers: {
            "Content-Type": "multipart/form-data",
          },
        }
      );

      setUploadResult(response.data);
      setFile(null);

      // Reset file input
      const fileInput = document.getElementById(
        "file-input"
      ) as HTMLInputElement;
      if (fileInput) fileInput.value = "";
    } catch (err: any) {
      setError(err.response?.data?.detail || err.message || "Upload failed");
    } finally {
      setUploading(false);
    }
  };

  const clearFile = () => {
    setFile(null);
    setUploadResult(null);
    setError(null);
    const fileInput = document.getElementById("file-input") as HTMLInputElement;
    if (fileInput) fileInput.value = "";
  };

  return (
    <div className="App">
      <div className="container">
        <header className="header">
          <div className="header-content">
            <h1>📊 Kafka CSV Producer</h1>
            <p>Upload CSV files to Apache Kafka</p>
          </div>
          <div className="status-badge">
            <div
              className={`status-indicator ${
                kafkaHealth?.status === "healthy" ? "healthy" : "unhealthy"
              }`}
            ></div>
            <span>
              {kafkaHealth?.status === "healthy"
                ? "Kafka Connected"
                : "Kafka Offline"}
            </span>
          </div>
        </header>

        <div className="upload-section">
          <div
            className={`drop-zone ${dragActive ? "drag-active" : ""} ${
              file ? "has-file" : ""
            }`}
            onDragEnter={handleDrag}
            onDragLeave={handleDrag}
            onDragOver={handleDrag}
            onDrop={handleDrop}
          >
            {!file ? (
              <>
                <div className="upload-icon">📁</div>
                <h3>Drag & Drop CSV File</h3>
                <p>or</p>
                <label htmlFor="file-input" className="btn btn-secondary">
                  Browse Files
                </label>
                <input
                  id="file-input"
                  type="file"
                  accept=".csv"
                  onChange={handleFileChange}
                  style={{ display: "none" }}
                />
                <p className="file-hint">Supported format: CSV</p>
              </>
            ) : (
              <div className="file-selected">
                <div className="file-icon">📄</div>
                <div className="file-info">
                  <h4>{file.name}</h4>
                  <p>{(file.size / 1024).toFixed(2)} KB</p>
                </div>
                <button
                  onClick={clearFile}
                  className="btn-remove"
                  title="Remove file"
                >
                  ✕
                </button>
              </div>
            )}
          </div>

          {file && !uploading && (
            <button onClick={handleUpload} className="btn btn-primary">
              Upload to Kafka
            </button>
          )}

          {uploading && (
            <div className="loading">
              <div className="spinner"></div>
              <p>Uploading to Kafka...</p>
            </div>
          )}
        </div>

        {error && (
          <div className="alert alert-error">
            <span className="alert-icon">⚠️</span>
            <span>{error}</span>
          </div>
        )}

        {uploadResult && (
          <div className="result-section">
            <div className="alert alert-success">
              <span className="alert-icon">✅</span>
              <span>Successfully uploaded to Kafka!</span>
            </div>

            <div className="result-card">
              <h3>Upload Summary</h3>
              <div className="result-details">
                <div className="result-item">
                  <span className="label">Filename:</span>
                  <span className="value">{uploadResult.filename}</span>
                </div>
                <div className="result-item">
                  <span className="label">Rows Sent:</span>
                  <span className="value">{uploadResult.rows_sent}</span>
                </div>
                <div className="result-item">
                  <span className="label">Kafka Topic:</span>
                  <span className="value">{uploadResult.topic}</span>
                </div>
              </div>

              {uploadResult.sample_data &&
                uploadResult.sample_data.length > 0 && (
                  <div className="sample-data">
                    <h4>
                      Sample Data (First {uploadResult.sample_data.length} rows)
                    </h4>
                    <div className="data-preview">
                      {uploadResult.sample_data.map((item, index) => (
                        <div key={index} className="data-row">
                          <span className="row-number">
                            Row {item.row_number}:
                          </span>
                          <code>{JSON.stringify(item.data, null, 2)}</code>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
            </div>
          </div>
        )}

        <div className="info-section">
          <h3>ℹ️ How it works</h3>
          <div className="info-cards">
            <div className="info-card">
              <div className="step-number">1</div>
              <h4>Upload CSV</h4>
              <p>Select or drag & drop your CSV file</p>
            </div>
            <div className="info-card">
              <div className="step-number">2</div>
              <h4>Process</h4>
              <p>Each row is sent to Kafka topic</p>
            </div>
            <div className="info-card">
              <div className="step-number">3</div>
              <h4>Consume</h4>
              <p>Consumer API stores data in SQLite</p>
            </div>
          </div>
        </div>

        <footer className="footer">
          <p>Kafka Learning Project • FastAPI + React + Kafka</p>
          <div className="links">
            <a
              href={`${API_URL}/docs`}
              target="_blank"
              rel="noopener noreferrer"
            >
              API Docs
            </a>
            <a
              href="http://localhost:8080"
              target="_blank"
              rel="noopener noreferrer"
            >
              Kafka UI
            </a>
            <a
              href="http://localhost:8001/docs"
              target="_blank"
              rel="noopener noreferrer"
            >
              Consumer API
            </a>
          </div>
        </footer>
      </div>
    </div>
  );
}

export default App;
