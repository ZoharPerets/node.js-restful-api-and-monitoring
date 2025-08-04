const express = require('express');
const mysql = require('mysql2/promise');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const cors = require('cors');
const log4js = require('log4js');
const { Kafka } = require('kafkajs');

const app = express();
const PORT = 3002;
const JWT_SECRET = 'your-secret-key';

// Configure logging
log4js.configure({
  appenders: { console: { type: 'console' } },
  categories: { default: { appenders: ['console'], level: 'info' } }
});
const logger = log4js.getLogger();

// Middleware
app.use(cors());
app.use(express.json());

// Database connection
let db;
async function connectDB() {
  try {
    db = await mysql.createConnection({
      host: 'mysql',
      user: 'root',
      password: 'password',
      database: 'app_db'
    });
    logger.info('Connected to MySQL');
  } catch (error) {
    logger.error('Database connection failed:', error);
    setTimeout(connectDB, 5000); // Retry after 5 seconds
  }
}

// Kafka setup
const kafka = new Kafka({
  clientId: 'server',
  brokers: ['kafka:9092']
});
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'app-group' });

async function connectKafka() {
  try {
    await producer.connect();
    await consumer.connect();
    logger.info('Connected to Kafka');
    
    // Start consumer in background
    startConsumer();
  } catch (error) {
    logger.error('Kafka connection failed:', error);
  }
}

// Consumer function (integrated into server)
async function startConsumer() {
  try {
    await consumer.subscribe({ topics: ['user-activity', 'database-changes'] });
    
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const data = JSON.parse(message.value.toString());
        
        const processedLog = {
          timestamp: new Date().toISOString(),
          topic: topic,
          data: data,
          processedBy: 'integrated-consumer'
        };
        
        logger.info('Processed Message:', JSON.stringify(processedLog, null, 2));
      },
    });
  } catch (error) {
    logger.error('Consumer error:', error);
  }
}

// Send message to Kafka
async function sendToKafka(topic, message) {
  try {
    await producer.send({
      topic: topic,
      messages: [{ value: JSON.stringify(message) }]
    });
  } catch (error) {
    logger.error('Failed to send Kafka message:', error);
  }
}

// Auth middleware
const auth = async (req, res, next) => {
  const token = req.header('Authorization')?.replace('Bearer ', '');
  
  if (!token) {
    return res.status(401).json({ error: 'No token provided' });
  }
  
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    const [rows] = await db.execute('SELECT * FROM users WHERE id = ?', [decoded.userId]);
    
    if (rows.length === 0) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    
    req.user = rows[0];
    next();
  } catch (error) {
    res.status(401).json({ error: 'Invalid token' });
  }
};

// Routes
app.post('/api/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    
    // Get user from database
    const [rows] = await db.execute('SELECT * FROM users WHERE email = ?', [email]);
    
    if (rows.length === 0) {
      return res.status(400).json({ error: 'Invalid credentials' });
    }
    
    const user = rows[0];
    
    // Check password
    const isValid = await bcrypt.compare(password, user.password);
    if (!isValid) {
      return res.status(400).json({ error: 'Invalid credentials' });
    }
    
    // Create token
    const token = jwt.sign({ userId: user.id }, JWT_SECRET, { expiresIn: '24h' });
    
    // Store token in database
    await db.execute(
      'INSERT INTO user_tokens (user_id, token, expires_at) VALUES (?, ?, DATE_ADD(NOW(), INTERVAL 24 HOUR))',
      [user.id, token]
    );
    
    // Log user activity
    const logEntry = {
      timestamp: new Date().toISOString(),
      userId: user.id,
      action: 'login',
      ipAddress: req.ip
    };
    logger.info('User Activity:', logEntry);
    
    // Send to Kafka
    await sendToKafka('user-activity', logEntry);
    
    // Log database change
    const dbChange = {
      timestamp: new Date().toISOString(),
      operation: 'INSERT',
      table: 'user_tokens',
      userId: user.id
    };
    await sendToKafka('database-changes', dbChange);
    
    res.json({
      token,
      user: { id: user.id, email: user.email }
    });
    
  } catch (error) {
    logger.error('Login error:', error);
    res.status(500).json({ error: 'Server error' });
  }
});

app.get('/api/profile', auth, (req, res) => {
  res.json({
    id: req.user.id,
    email: req.user.email
  });
});

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'OK' });
});

// Start server
async function startServer() {
  await connectDB();
  await connectKafka();
  
  app.listen(PORT, () => {
    logger.info(`Server running on port ${PORT}`);
  });
}

startServer();