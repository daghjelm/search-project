document.getElementById('searchQuery').addEventListener('keypress', function (event) {
    if (event.key === 'Enter') {
        sendSearch(); // Call the search function
    }
});


function sendSearch() {
    const query = document.getElementById('searchQuery').value;
    fetch(`${config.baseURL}/search`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ query: query })
    }).then(response => response.json())
        .then(data => {
            const resultsDiv = document.getElementById('results');
            resultsDiv.innerHTML = ''; // Clear previous results
            let hits = data.hits || [];

            let displayIndex = 0; // Starting index to display results

            function displayResults() {
                const endIndex = Math.min(displayIndex + 5, hits.length);
                for (let i = displayIndex; i < endIndex; i++) {
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
                displayIndex += 5;

                // Check if "Show More" button is needed
                if (displayIndex < hits.length) {
                    //showMoreButton.style.display = 'block';
                    resultsDiv.appendChild(moreResultsDiv); // Move the button after adding results
                } else {
                    showMoreButton.style.display = 'none';
                }
            }

            // Initialize and append "Show More" button
            
            const showMoreButton = document.createElement('button');
            showMoreButton.className = "search-box-button"
            showMoreButton.textContent = 'Show More';
            
            const moreResultsDiv = document.createElement('div');
            moreResultsDiv.appendChild(showMoreButton)
            moreResultsDiv.className = "show-more-container"

            showMoreButton.onclick = displayResults;

            // Display initial results
            displayResults();
        })
        .catch(error => console.error('Error:', error));
}

function highlightText(transcript, indices) {
    let lastEnd = 0;
    let highlightedHTML = '';
    indices.forEach(index => {
        highlightedHTML += transcript.slice(lastEnd, index[0]);
        highlightedHTML += `<span class="highlight">${transcript.slice(index[0], index[1])}</span>`;
        lastEnd = index[1];
    });
    highlightedHTML += transcript.slice(lastEnd);
    return highlightedHTML;
}
