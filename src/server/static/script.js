document.getElementById('searchQuery').addEventListener('keypress', function (event) {
    if (event.key === 'Enter') {
        sendSearch(); // Call the search function
    }
});


function sendSearch() {
    const query = document.getElementById('searchQuery').value;
    const minutes = document.getElementById('duration').value;
    const weighted = document.getElementById('weighted').value;
    fetch(`${config.baseURL}/search`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ query, minutes, weighted})
    }).then(response => response.json())
        .then(data => {
            displayResults(data);
        })
        .catch(error => console.error('Error:', error));
}

function displayResults(data) {
    console.log(data)
    const resultsDiv = document.getElementById('results');
    resultsDiv.innerHTML = ''; // Clear previous results
    let hits = data.hits || [];

    const numResultsDiv = document.createElement('div');
    numResultsDiv.className = 'num-results-container';
    numResultsDiv.innerHTML = 'Number of results: ' + data.numHits 
    resultsDiv.appendChild(numResultsDiv);
    
    for (let i = 0; i < data.hits.length; i++) {
        const hit = hits[i];
        const hitDiv = document.createElement('div');

        const podcastNameDiv = document.createElement('div');
        podcastNameDiv.className = 'title'; // Add this class
        podcastNameDiv.textContent = `Podcast name: ${hit.podcast}`;
        hitDiv.appendChild(podcastNameDiv);

        const episodeNameDiv = document.createElement('div');
        episodeNameDiv.className = 'title'; // Add this class
        episodeNameDiv.textContent = `Episode: ${hit.episode}`;
        hitDiv.appendChild(episodeNameDiv);

        const transcriptDiv = document.createElement('div');
        transcriptDiv.innerHTML = `Transcript: ${highlightText(hit.transcript, hit.indices)}`;
        hitDiv.appendChild(transcriptDiv);

        const startTimeDiv = document.createElement('div');
        startTimeDiv.textContent = `Start time for above transcript in the podcast: ${hit.startTime}`;
        hitDiv.appendChild(startTimeDiv);

        resultsDiv.appendChild(hitDiv);
    }
       
    if (hits.length > 0 && hits.length < data.numHits) {
        const showMoreButton = document.createElement('button');
        showMoreButton.className = "search-box-button"
        showMoreButton.textContent = 'Show More';

        const moreResultsDiv = document.createElement('div');
        moreResultsDiv.appendChild(showMoreButton)
        moreResultsDiv.className = "show-more-container"
        resultsDiv.appendChild(moreResultsDiv); // Move the button after adding results
        
        // Initialize and append "Show More" button
        showMoreButton.onclick = get_more_results;
    }

       

}


function get_more_results(){
    const query = document.getElementById('searchQuery').value;
    const minutes = document.getElementById('duration').value;
    const weighted = document.getElementById('weighted').value;
    fetch(`${config.baseURL}/get-next`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ query, minutes, weighted})
    }).then(response => response.json())
        .then(data => {
            displayResults(data);
        })
}

function highlightText(transcript, indices) {
    indices.sort((a, b) => a[0] - b[0]);  // Sort indices to handle them in order

    let lastEnd = 0;
    let highlightedHTML = '';

    indices.forEach(([start, end]) => {
        if (start < lastEnd) {
            // Adjust start to avoid overlapping highlights
            start = lastEnd;
        }
        highlightedHTML += transcript.slice(lastEnd, start);
        highlightedHTML += `<span class="highlight">${transcript.slice(start, end)}</span>`;
        lastEnd = end;
    });

    highlightedHTML += transcript.slice(lastEnd);
    return highlightedHTML;
}
