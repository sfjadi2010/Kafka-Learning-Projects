# Quick Start Guide - React Frontend

## 🎉 Your React UI is now running!

### Access the Application

**Main UI (React Frontend)**: http://localhost:3000

### Features

✅ **Drag & Drop Upload** - Simply drag CSV files onto the upload area
✅ **File Browser** - Or click to browse and select files
✅ **Real-time Status** - See Kafka connection status at a glance
✅ **Upload Progress** - Visual feedback during upload
✅ **Results Display** - View upload summary and sample data
✅ **Responsive Design** - Works on desktop and mobile

### How to Use

1. **Open the app**: Navigate to http://localhost:3000 in your browser

2. **Upload a CSV file**:
   - Drag and drop a CSV file onto the upload area, OR
   - Click "Browse Files" to select a file

3. **Submit**: Click "Upload to Kafka" button

4. **View Results**: See the upload summary with:
   - Number of rows sent to Kafka
   - Topic name
   - Sample data preview

5. **Start Consumer** (Optional):
   - Open http://localhost:8001/docs
   - Use the `/start-consumer` endpoint to begin consuming messages
   - Check stored data at `/records` endpoint

### Testing with Sample Data

Use the included `sample_data.csv` file:
- Contains 10 sample user records
- Perfect for testing the upload functionality

### Architecture

```
User Browser (Port 3000)
    ↓
React Frontend
    ↓
Producer API (Port 8000)
    ↓
Kafka (Port 9092/9093)
    ↓
Consumer API (Port 8001)
    ↓
SQLite Database
```

### Additional Endpoints

- **Kafka UI**: http://localhost:8080 - Monitor Kafka topics and messages
- **Producer API Docs**: http://localhost:8000/docs - REST API documentation
- **Consumer API Docs**: http://localhost:8001/docs - Consumer API documentation

### Troubleshooting

**Frontend won't load?**
- Check: `docker logs kafka-frontend`
- Ensure port 3000 is not in use

**Can't upload files?**
- Check Kafka connection status (should show "Kafka Connected")
- Verify Producer API is running: http://localhost:8000/health

**CORS errors?**
- Producer API has CORS enabled for all origins
- Check browser console for specific errors

### Development Mode

To run the frontend locally (without Docker):

```bash
cd frontend
npm install
npm start
```

The app will open at http://localhost:3000 and connect to the Docker APIs.

---

Enjoy your Kafka learning journey! 🚀
