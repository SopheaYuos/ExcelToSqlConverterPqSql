const xlsx = require('xlsx');
const { Client } = require('pg');
const fs = require('fs');

// Database connection setup
const client = new Client({
  user: 'postgres',
  host: 'localhost',
  database: 'infinity_live_backup',
  password: 'admin',
  port: 5432,
});

// Function to retrieve database columns dynamically
async function getDatabaseColumns() {
  try {
    await client.connect();

    // Query to get column names from the database table
    const res = await client.query(`
      SELECT column_name 
      FROM information_schema.columns
      WHERE table_name = 'HCIPClaims';`); // Adjust table name accordingly

    const dbColumns = res.rows.map((row) => row.column_name);
    console.log('Database Columns:', dbColumns);

    return dbColumns;
  } catch (err) {
    console.error('Error retrieving columns:', err);
  } finally {
    await client.end();
  }
}

// Function to convert Excel serial date to a JavaScript Date object
function excelDateToJSDate(excelDate) {
  const excelStartDate = new Date(1900, 0, 1); // January 1, 1900
  return new Date(
    excelStartDate.getTime() + (excelDate - 2) * 24 * 60 * 60 * 1000
  ); // Adjust for Excel's leap year bug
}

// Function to format date as 'YYYY-MM-DD'
function formatDate(value) {
  // Check if the value is a number (Excel serial date)
  if (!isNaN(value)) {
    value = excelDateToJSDate(value);
  }

  const date = new Date(value);
  if (isNaN(date)) {
    return 'NULL'; // Return NULL if the date is invalid
  }
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0'); // Add leading zero if month is single digit
  const day = String(date.getDate()).padStart(2, '0'); // Add leading zero if day is single digit
  return `'${year}-${month}-${day}'`;
}

// Function to load Excel and map columns dynamically
async function mapExcelToDatabase() {
  try {
    // Retrieve columns from DB dynamically
    const dbColumns = await getDatabaseColumns();

    // Load Excel file
    const workbook = xlsx.readFile(
      'C:/Users/user/Downloads/claim_history.xlsx'
    );
    const sheet = workbook.Sheets[workbook.SheetNames[1]]; // Assuming data is in the second sheet
    const data = xlsx.utils.sheet_to_json(sheet);

    // Generate column mapping based on matching column names
    let columnMapping = {};
    const excelColumns = Object.keys(data[0]); // Extract column names from the first row

    // Find the matching columns
    excelColumns.forEach((excelCol) => {
      if (dbColumns.includes(excelCol)) {
        columnMapping[excelCol] = excelCol; // Mapping the matched columns directly
      }
    });

    console.log('Column Mapping:', columnMapping);

    // Map the data to database columns and format dates
    let sqlQueries = [];
    data.forEach((row) => {
      let values = [];
      let columns = [];

      for (let excelCol in columnMapping) {
        let dbCol = columnMapping[excelCol];
        let value = row[excelCol];

        if (value !== undefined) {
          // Check if the value is a date and format it
          if (
            dbCol === 'ClaimDate' ||
            dbCol === 'CheckIn' ||
            dbCol === 'CheckOut'
          ) {
            // Format the date to 'YYYY-MM-DD' using custom formatDate function
            value = formatDate(value);
          } else {
            value = `'${value}'`; // Wrap other values in quotes
          }

          values.push(value);
          columns.push(`"${dbCol}"`);
        }
      }

      if (columns.length > 0) {
        // Format the SQL query to match the desired structure
        let sql = `INSERT INTO data."HCIPClaims" (\n  ${columns.join(
          ', \n '
        )}) \nVALUES \n  (${values.join(', ')})`;
        sqlQueries.push(sql);
      }
    });

    // Output the SQL queries to a file
    fs.writeFileSync('output.sql', sqlQueries.join(';\n\n') + ';');
  } catch (err) {
    console.error('Error processing Excel file:', err);
  }
}

mapExcelToDatabase();
