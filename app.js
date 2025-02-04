const express = require('express');
const app = express();
const cors = require('cors');

const pgp = require('pg-promise')();
const db = pgp('postgresql://postgres:7263@localhost:5432/mine');
app.use(cors());
// Define your API endpoint
app.get('/api/data', async (req, res) => {
    try {
        // Query the database
        const data = await db.any('SELECT * FROM f');

        // Send the response
        res.json(data);
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Start the server
app.listen(3000, () => {
    console.log('Server is running on port 3000');
});
