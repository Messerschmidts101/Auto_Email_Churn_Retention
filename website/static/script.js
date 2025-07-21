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
    const spinner = document.getElementById('loading-spinner-training-details-preview');
    spinner.style.display = 'block'; // Show spinner
    const progressBarContainer = document.getElementById('progress-bar-container-train');
    const progressBar = document.getElementById('progress-bar-train');
    const progressLabel = document.getElementById('progress-label-train');

    // Show loading UI
    spinner.style.display = 'block';
    progressBarContainer.style.display = 'block';

    // Start fake progress
    let percent = 0;
    const totalDuration = 900000; // 10 minutes in milliseconds
    const interval = 1000;         // update every 1 second
    const increment = 100 / (totalDuration / interval);

    const timer = setInterval(() => {
        percent = Math.min(100, percent + increment);
        progressBar.style.width = `${percent}%`;
        progressLabel.textContent = `${Math.floor(percent)}%`;
    }, interval);
    fetch('/train_model')
        .then(res => res.json())
        .then(data => {
            
            // Conclude timer
            clearInterval(timer);
            percent = 100;
            progressBar.style.width = `100%`;
            progressLabel.textContent = `100%`;
            alert('training finished.');
            spinner.style.display = 'none';
            progressBarContainer.style.display = 'none';

            // Show Tables
            displayTable(data.samples, "training-details-preview");
            displayTable(data.metrics, 'metrics-details-preview');
            displayTable(data.confusion_matrix, 'confusion-metrix-details-preview');
            document.getElementById('time-details').textContent = `Time Taken: ${data.time} seconds`;

            // Enable the Step 3 button after training finishes
            const proceedButton = document.getElementById('btn-proceed-inference');
            proceedButton.disabled = false;
            proceedButton.style.backgroundColor = '#4caf50';
            proceedButton.style.cursor = 'pointer';
            proceedButton.removeAttribute('title');

        })
        .catch(err => {
            console.error('Training error:', err);
            alert('Model training failed.');
        }).finally(() => {
            spinner.style.display = 'none'; // Hide spinner
        });
}

// Run inference
function inferenceModel() {
    const spinner = document.getElementById('loading-spinner-churn-report-result');
    const progressBarContainer = document.getElementById('progress-bar-container-infer');
    const progressBar = document.getElementById('progress-bar-infer');
    const progressLabel = document.getElementById('progress-label-infer');

    // Show loading UI
    spinner.style.display = 'block';
    progressBarContainer.style.display = 'block';

    // Start fake progress
    let percent = 0;
    const totalDuration = 900000; // 10 minutes in milliseconds
    const interval = 1000;         // update every 1 second
    const increment = 100 / (totalDuration / interval);

    const timer = setInterval(() => {
        percent = Math.min(100, percent + increment);
        progressBar.style.width = `${percent}%`;
        progressLabel.textContent = `${Math.floor(percent)}%`;
    }, interval);
    fetch('/run_inference')
        .then(res => res.json())
        .then(data => {
            
            // Conclude timer
            clearInterval(timer);
            percent = 100;
            progressBar.style.width = `100%`;
            progressLabel.textContent = `100%`;
            alert('training finished.');
            spinner.style.display = 'none';
            progressBarContainer.style.display = 'none';

            // Display table
            alert('Inference complete.');
            displayTable(data, 'churn-report-result');
        })
        .catch(err => {
            console.error('Inference error:', err);
            alert('Inference failed.');
        })
        .finally(() => {
            spinner.style.display = 'none'; // Hide spinner
        });
}

// Create Emails
function createEmail() {
    const spinner = document.getElementById('loading-spinner-email-generation-result'); 
    spinner.style.display = 'block'; // Show spinner
    fetch('/create_emails') 
        .then(res => res.json())
        .then(data => {
            alert('email generation complete.');
            displayTable(data, 'email-generation-result'); 

            // Enable the Step 4 button after training finishes
            const sendEmailButton = document.getElementById('btn-send-email');
            sendEmailButton.disabled = false;
            sendEmailButton.style.backgroundColor = '#4caf50';
            sendEmailButton.style.cursor = 'pointer';
            sendEmailButton.removeAttribute('title');
        })
        .catch(err => {
            console.error('Email generation error:', err);
            alert('Email generation failed.');
        })
        .finally(() => {
            spinner.style.display = 'none'; // Hide spinner
        });
}

function sendEmail() {
    const spinner = document.getElementById('loading-spinner-send-email-result'); 
    const btn = document.getElementById('btn-send-email');
    const status = document.getElementById('send-email-status');

    spinner.style.display = 'block'; 
    btn.textContent = 'Sending...';
    btn.disabled = true;

    fetch('/send_emails') 
        .then(res => res.json())
        .then(data => {
            status.textContent = '✅ Emails sent successfully!';
            status.style.color = 'green';
        })
        .catch(err => {
            console.error('Email sending error:', err);
            status.textContent = '❌ Failed to send emails.';
            status.style.color = 'red';
        })
        .finally(() => {
            spinner.style.display = 'none'; 
            btn.textContent = 'Send Emails To All Customers';
            btn.disabled = false;
        });
}


// Display data table
function displayTable(data, targetId) {
    const container = document.getElementById(targetId);    
    /*
    ########################################################
    #######                                          #######
    #######         Step 1: Display Table            #######
    #######                                          #######
    ########################################################
    */
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

    /*
    ########################################################
    #######                                          #######
    #######      Step 2: Create Download Button      #######
    #######                 of Table                 #######
    #######                                          #######
    ########################################################
    */

    // 1. Create the button
    let filename = `${targetId}.csv`;
    const button = document.createElement('button');
    button.style.cssText = 'float: right; margin: 10px 0;'; // Button will be placed at right
    button.textContent = 'Download Table';  // Button label
    button.onclick = function () {
        const header = Object.keys(data[0]);
        const csvRows = [header.join(',')];

        data.forEach(row => {
            const values = header.map(key => 
                `"${String(row[key]).replace(/"/g, '""')}"` // escape quotes
            );
            csvRows.push(values.join(','));
        });

        const csvContent = csvRows.join('\n');
        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });

        const link = document.createElement('a');
        link.href = URL.createObjectURL(blob);
        link.setAttribute('download', filename);
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    };

    // 2. Append the button to the div
    container.appendChild(document.createElement('br'));
    container.appendChild(button);


}

