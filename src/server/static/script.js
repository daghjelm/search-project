function sendSearch() {
    const query = document.getElementById('searchQuery').value;
    fetch(`${config.baseURL}/search`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({query: query})
    }).then(response => response.json())
    .then(data => {
        const resultsDiv = document.getElementById('results');
        resultsDiv.innerHTML = ''; // Clear previous results
        console.log(data)
        if (data.hits) {
            data.hits.forEach(hit => {
                const div = document.createElement('div');
                div.textContent = hit; 
                resultsDiv.appendChild(div);
            });
        }
    })
    .catch(error => console.error('Error:', error));
}