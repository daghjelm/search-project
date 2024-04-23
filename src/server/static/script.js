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
                if (hit.indices.length > 0) {
                    // Create a div for this hit
                    const hitDiv = document.createElement('div');
                    
                    // Create a div for each field and append it to hitDiv
                    const field1Div = document.createElement('div');
                    field1Div.textContent = `Podcast name: ${hit.podcast}`;
                    hitDiv.appendChild(field1Div);

                    const field2Div = document.createElement('div');
                    field2Div.innerHTML = `Transcript: ${highlightText(hit.transcript, hit.indices)}`;
                    hitDiv.appendChild(field2Div);

                    const field3Div = document.createElement('div');
                    field3Div.textContent = `Start time for above transcript in the podcast: ${hit.startTime}`;
                    hitDiv.appendChild(field3Div);

                    const field4Div = document.createElement('div');
                    field4Div.textContent = `End time for above transcript in the podcast: ${hit.endTime}`;
                    hitDiv.appendChild(field4Div);

                    // Append the hitDiv to the main resultsDiv
                    resultsDiv.appendChild(hitDiv);
                }
            });
        }
    })
    .catch(error => console.error('Error:', error));
}

// This function creates HTML with highlighted text based on the provided indices
function highlightText(transcript, indices) {
    console.log(indices); // Check what indices look like here
    let lastEnd = 0;
    let highlightedHTML = '';

    // Loop through each index range and build the HTML string
    indices.forEach(index => {
        // Add text before the highlight
        highlightedHTML += transcript.slice(lastEnd, index[0]);
        // Add the highlighted text
        highlightedHTML += `<span class="highlight">${transcript.slice(index[0], index[1])}</span>`;
        lastEnd = index[1];
    });
    // Add any remaining text after the last highlight
    highlightedHTML += transcript.slice(lastEnd);

    return highlightedHTML;
}