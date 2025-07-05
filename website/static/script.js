function showSection(sectionId) {
    document.querySelectorAll('.section').forEach(section => {
        section.classList.add('hidden');
    });
    document.getElementById(sectionId).classList.remove('hidden');
}

// Upload CSV handler with dynamic preview
function uploadCSV(type) {
    const fileInput = document.getElementById(`${type}-file`);
    const file = fileInput?.files?.[0];
    if (!file) {
        alert("Please select a CSV file.");
        return;
    }

    const formData = new FormData();
    formData.append('file', file);

    fetch(`/upload_${type}`, {
        method: 'POST',
        body: formData
    })
        .then(res => res.json())
        .then(data => {
            alert(`${type} CSV uploaded.`);
            const target = type === 'train' ? 'uploaded-train-preview' : 'uploaded-scoring-preview';
            displayTable(data, target);
        })
        .catch(err => {
            console.error('Upload error:', err);
            alert(`Upload failed for ${type}.`);
        });
}

// Train model
function trainModel() {
    // TODO: DISPLAY METRICS HERE
    fetch('/train_model')
        .then(res => res.json())
        .then(data => {
            alert('training finished.');
            displayTable(data, "training-details-preview");
        })
        .catch(err => {
            console.error('Training error:', err);
            alert('Model training failed.');
            });
}

// Run inference
function inferenceModel() {
fetch('/run_inference')
    .then(res => res.json())
    .then(data => {
    alert('Inference complete.');
    displayTable(data, 'uploaded-scoring-preview');
    })
    .catch(err => {
    console.error('Inference error:', err);
    alert('Inference failed.');
    });
}

// Display data table
function displayTable(data, targetId) {
    const container = document.getElementById(targetId);
    container.innerHTML = '';

    if (!Array.isArray(data) || data.length === 0) {
        container.innerHTML = '<p>No data to display.</p>';
        return;
    }

    const table = document.createElement('table');
    table.style.width = '100%';
    table.style.borderCollapse = 'collapse';

    // Header
    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');
    Object.keys(data[0]).forEach(key => {
        const th = document.createElement('th');
        th.textContent = key;
        th.style.cssText = 'border:1px solid #ccc;padding:6px;background:#f2f2f2;position:sticky;top:0';
        headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);
    table.appendChild(thead);

    // Body
    const tbody = document.createElement('tbody');
    data.forEach(row => {
        const tr = document.createElement('tr');
        Object.values(row).forEach(cell => {
        const td = document.createElement('td');
        td.textContent = cell;
        td.style.cssText = 'border:1px solid #ccc;padding:6px;';
        tr.appendChild(td);
        });
        tbody.appendChild(tr);
    });

    table.appendChild(tbody);
    container.appendChild(table);
}
