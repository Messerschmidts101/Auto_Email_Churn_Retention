const API_CONFIG = {
    train: {
        inputId: 'train-file',
        previewId: 'uploaded-train-preview',
        uploadUrl: '/train/upload',
        uploadLabel: 'training'
    },
    score: {
        inputId: 'score-file',
        previewId: 'uploaded-scoring-preview',
        uploadUrl: '/score/upload',
        uploadLabel: 'scoring'
    }
};

function showSection(sectionId) {
    document.querySelectorAll('.section').forEach(section => {
        section.classList.add('hidden');
    });
    document.getElementById(sectionId).classList.remove('hidden');
}

function normalizeTableData(data) {
    if (Array.isArray(data)) {
        return data;
    }
    if (data && typeof data === 'object') {
        return [data];
    }
    return [];
}

function toNumber(id, fallbackValue) {
    const rawValue = document.getElementById(id)?.value;
    const parsedValue = Number(rawValue);
    return Number.isFinite(parsedValue) ? parsedValue : fallbackValue;
}

function buildMetadataLine(timeTaken, dateCreated) {
    const fragments = [];

    if (timeTaken !== undefined && timeTaken !== null && timeTaken !== '') {
        fragments.push(`Time Taken: ${timeTaken} seconds`);
    }
    if (dateCreated) {
        fragments.push(`Date Created: ${dateCreated}`);
    }

    return fragments.join(' | ');
}

function setButtonEnabled(buttonId, enabled) {
    const button = document.getElementById(buttonId);
    if (!button) {
        return;
    }

    button.disabled = !enabled;
    button.style.backgroundColor = enabled ? '#4caf50' : '#ccc';
    button.style.cursor = enabled ? 'pointer' : 'not-allowed';

    if (enabled) {
        button.removeAttribute('title');
    }
}

function startProgress(spinnerId, containerId, barId, labelId) {
    const spinner = document.getElementById(spinnerId);
    const container = document.getElementById(containerId);
    const bar = document.getElementById(barId);
    const label = document.getElementById(labelId);

    spinner.style.display = 'block';
    container.style.display = 'block';
    bar.style.width = '0%';
    label.textContent = '0%';

    let percent = 0;
    const totalDuration = 900000;
    const interval = 1000;
    const increment = 100 / (totalDuration / interval);

    const timer = setInterval(() => {
        percent = Math.min(100, percent + increment);
        bar.style.width = `${percent}%`;
        label.textContent = `${Math.floor(percent)}%`;
    }, interval);

    return { spinner, container, bar, label, timer };
}

function stopProgress(progressState, markComplete = false) {
    if (!progressState) {
        return;
    }

    clearInterval(progressState.timer);
    progressState.bar.style.width = markComplete ? '100%' : '0%';
    progressState.label.textContent = markComplete ? '100%' : '0%';
    progressState.spinner.style.display = 'none';
    progressState.container.style.display = 'none';
}

async function fetchJson(url, options = {}, fallbackError = 'Request failed.') {
    const response = await fetch(url, options);
    let data = null;

    try {
        data = await response.json();
    } catch (error) {
        data = null;
    }

    if (!response.ok) {
        const detail = Array.isArray(data?.detail)
            ? data.detail.map(item => item.msg).join(', ')
            : data?.detail;
        const statusText = data?.dicStatus ? JSON.stringify(data.dicStatus) : '';
        throw new Error(detail || statusText || fallbackError);
    }

    return data;
}

async function uploadCSV(type) {
    const config = API_CONFIG[type];
    const fileInput = document.getElementById(config?.inputId);
    const file = fileInput?.files?.[0];

    if (!config) {
        alert('Unknown upload type.');
        return;
    }

    if (!file) {
        alert('Please select a CSV file.');
        return;
    }

    const formData = new FormData();
    formData.append('objFile', file);

    try {
        const data = await fetchJson(
            config.uploadUrl,
            {
                method: 'POST',
                body: formData
            },
            `Upload failed for ${config.uploadLabel}.`
        );

        alert(`${config.uploadLabel} CSV uploaded.`);
        displayTable(data.tblOutput, config.previewId);
    } catch (error) {
        console.error('Upload error:', error);
        alert(error.message || `Upload failed for ${config.uploadLabel}.`);
    }
}

function getTrainingRequestBody() {
    return {
        intRandomState: toNumber('train-random-state', 0),
        intTopFeats: toNumber('train-top-feats', 20),
        fltF1: toNumber('train-f1-threshold', 1)
    };
}

async function trainModel() {
    const progress = startProgress(
        'loading-spinner-training-details-preview',
        'progress-bar-container-train',
        'progress-bar-train',
        'progress-label-train'
    );
    const timeDetails = document.getElementById('time-details');
    const featureImportanceSection = document.getElementById('feature-importance-section');
    const featureImportancePreview = document.getElementById('feature-importance-preview');

    try {
        const data = await fetchJson(
            '/train/model',
            {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(getTrainingRequestBody())
            },
            'Model training failed.'
        );

        stopProgress(progress, true);
        alert('Training finished.');

        displayTable(data.objDatasetSplit, 'training-details-preview');
        displayTable(data.objMetrics, 'metrics-details-preview');
        displayTable(data.objConfusionMatrix, 'confusion-metrix-details-preview');
        timeDetails.textContent = buildMetadataLine(data.timeTaken, data.dateCreated);

        if (Array.isArray(data.tblFeatureImportance) && data.tblFeatureImportance.length > 0) {
            featureImportanceSection.classList.remove('hidden');
            displayTable(data.tblFeatureImportance, 'feature-importance-preview');
        } else if (featureImportanceSection && featureImportancePreview) {
            featureImportanceSection.classList.add('hidden');
            featureImportancePreview.innerHTML = '';
        }

        setButtonEnabled('btn-proceed-inference', true);
    } catch (error) {
        console.error('Training error:', error);
        alert(error.message || 'Model training failed.');
        stopProgress(progress, false);
    }
}

async function inferenceModel() {
    const progress = startProgress(
        'loading-spinner-inference-progress',
        'progress-bar-container-infer',
        'progress-bar-infer',
        'progress-label-infer'
    );
    const timeDetails = document.getElementById('inference-time-details');

    try {
        const data = await fetchJson(
            '/score/model',
            {
                method: 'POST'
            },
            'Inference failed.'
        );

        stopProgress(progress, true);
        alert('Inference complete.');
        displayTable(data.tblOutput, 'churn-report-result');
        timeDetails.textContent = buildMetadataLine(data.timeTaken, data.dateCreated);
    } catch (error) {
        console.error('Inference error:', error);
        alert(error.message || 'Inference failed.');
        stopProgress(progress, false);
    }
}

async function createEmail() {
    const spinner = document.getElementById('loading-spinner-email-generation-result');
    spinner.style.display = 'block';

    try {
        const data = await fetchJson('/create_emails', {}, 'Email generation failed.');
        alert('Email generation complete.');
        displayTable(data.tblOutput || data, 'email-generation-result');
        setButtonEnabled('btn-send-email', true);
    } catch (error) {
        console.error('Email generation error:', error);
        alert(error.message || 'Email generation failed.');
    } finally {
        spinner.style.display = 'none';
    }
}

async function viewResults() {
    const spinner = document.getElementById('loading-spinner-view-results');
    const rowCountDiv = document.getElementById('view-result-result-row-count');
    const strTableName = document.getElementById('table-type').value;
    const strTableVersion = document.getElementById('version-type').value;

    spinner.style.display = 'block';

    try {
        const query = new URLSearchParams({
            strTableName,
            strTableVersion
        });
        const data = await fetchJson(
            `/database/table?${query.toString()}`,
            {
                method: 'GET'
            },
            'Unable to load results.'
        );

        rowCountDiv.innerText = `Row count: ${data.intRowCount}`;
        displayTable(data.tblOutput, 'view-result-result');
    } catch (error) {
        console.error('View results error:', error);
        rowCountDiv.innerText = error.message || 'Unable to load results.';
        document.getElementById('view-result-result').innerHTML = '';
    } finally {
        spinner.style.display = 'none';
    }
}

async function sendEmail() {
    const spinner = document.getElementById('loading-spinner-send-email-result');
    const button = document.getElementById('btn-send-email');
    const status = document.getElementById('send-email-status');

    spinner.style.display = 'block';
    button.textContent = 'Sending...';
    button.disabled = true;

    try {
        await fetchJson('/send_emails', {}, 'Failed to send emails.');
        status.textContent = 'Emails sent successfully.';
        status.style.color = 'green';
    } catch (error) {
        console.error('Email sending error:', error);
        status.textContent = error.message || 'Failed to send emails.';
        status.style.color = 'red';
    } finally {
        spinner.style.display = 'none';
        button.textContent = 'Send Emails To All Customers';
        button.disabled = false;
    }
}

function displayTable(data, targetId) {
    const container = document.getElementById(targetId);
    const rows = normalizeTableData(data);

    container.innerHTML = '';

    if (rows.length === 0) {
        container.innerHTML = '<p>No data to display.</p>';
        return;
    }

    const table = document.createElement('table');
    table.style.width = '100%';
    table.style.borderCollapse = 'collapse';

    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');
    Object.keys(rows[0]).forEach(key => {
        const th = document.createElement('th');
        th.textContent = key;
        th.style.cssText = 'border:1px solid #ccc;padding:6px;background:#f2f2f2;position:sticky;top:0';
        headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);
    table.appendChild(thead);

    const tbody = document.createElement('tbody');
    rows.forEach(row => {
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

    const filename = `${targetId}.csv`;
    const button = document.createElement('button');
    button.style.cssText = 'float: right; margin: 10px 0;';
    button.textContent = 'Download Table';
    button.onclick = function () {
        const header = Object.keys(rows[0]);
        const csvRows = [header.join(',')];

        rows.forEach(row => {
            const values = header.map(key =>
                `"${String(row[key]).replace(/"/g, '""')}"`
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

    container.appendChild(document.createElement('br'));
    container.appendChild(button);
}
