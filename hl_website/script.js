const API_ENDPOINT = 'http://localhost:5000/funds';

let fundsData = []; // Store the fetched data globally

async function fetchData(orderBy = 'fund_name', orderDir = 'asc', filterTerm = '') {
    let url = `${API_ENDPOINT}?order_by=${orderBy}&order_dir=${orderDir}&filter=${filterTerm}`;
    try {
        const response = await fetch(url);
        const data = await response.json();
        fundsData = data; // Store the fetched data
        populateTable(data);
    } catch (error) {
        console.error('Error fetching data:', error);
    }
}

function populateTable(data) {
    const tableBody = document.querySelector('#funds-table tbody');
    tableBody.innerHTML = ''; // Clear the table before populating
    data.forEach(fund => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${fund.sedol}</td>
            <td>${fund.fund_name}</td>
            <td>${fund.company_name}</td>
            <td>${fund.annual_charge}</td>
            <td>${fund.fund_size}</td>
            <td>${fund.perf12m}</td>
            <td>${fund.yield}</td>
            <td>${fund.bid_price}</td>
            <td>${fund.offer_price}</td>
            <td><a href="${fund.kiid_url}" target="_blank">KIID</a></td>
            <td>${fund.updated}</td>
        `;
        tableBody.appendChild(row);
    });
}

// Initial data load
fetchData();

// Event listeners for sorting and filtering
document.getElementById('sort-button').addEventListener('click', () => {
    const orderBy = document.getElementById('order-select').value;
    const orderDir = 'asc'; // You can add a toggle for ascending/descending
    const filterTerm = document.getElementById('filter-input').value;
    fetchData(orderBy, orderDir, filterTerm);
});

document.getElementById('filter-input').addEventListener('input', () => {
    const orderBy = document.getElementById('order-select').value;
    const orderDir = 'asc';
    const filterTerm = document.getElementById('filter-input').value;
    fetchData(orderBy, orderDir, filterTerm);
});
