const express = require('express');
const app = express();
const pgp = require('pg-promise')();
const db = pgp('postgres://username:7263@localhost:5432/database');

// Define your API endpoint
app.get('/api/data', async (req, res) => {
    try {
        // Query the database
        const data = await db.any('SELECT * FROM reminder');

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
