const uploadDataset = () => {
    const select = document.getElementById('dataset')
    const input_file = document.getElementById("file");
	const response_text_element_title = document.getElementById("response-title");
	const response_text_element = document.getElementById("response");

    response_text_element.innerHTML = "Uploading..."
    response_text_element.classList.remove("response-msg--error");
    response_text_element.classList.remove("response-msg--success");

    var data = new FormData()
    data.append('file', input_file.files[0])

    fetch("datasets/" + select.value, {
        method: "POST",
        body: data
    }).then(response => response.json()
    .then(result => {
        if(response.ok) {
            response_text_element.innerHTML = `File uploaded: ${result['details']}`
            response_text_element.classList.add("response-msg--success");
            response_text_element.classList.remove("response-msg--error");
        } else {
        	error_detail = "<ul>";
        	if (Array.isArray(result['details'])) {
        		result['details'].forEach(error => {
        			error_detail += "<li>" + error + "</li>";
        		})
        	} else {
        		error_detail += "<li>" + result['details'] + "</li>";
        	}
			error_detail += "</ul>";

			response_text_element_title.innerHTML = "Errors:"
			response_text_element_title.classList.add("response-msg--error")

			response_text_element.innerHTML = error_detail;
            response_text_element.classList.add("response-msg--error");
            response_text_element.classList.remove("response-msg--success");
        }
    })).catch(() => {
    		response_text_element.innerHTML = "Error: There was a problem uploading the file";
            response_text_element.classList.add("response-msg--error");
            response_text_element.classList.remove("response-msg--success");
    })
}

document.getElementById("file").onchange = () => {
  document.getElementById("response-title").innerHTML = ""
  document.getElementById("response").innerHTML = "";
  document.getElementById("filename").textContent = document.getElementById("file").files[0].name;
}

document.getElementById("dataset").onchange = () => {
    document.getElementById("response-title").innerHTML = ""
    document.getElementById("response").innerHTML = "";
    document.getElementById("upload-dataset").textContent = 'Step 3: Hit "Upload dataset" button and the data will be uploaded to ' + document.getElementById("dataset").value;
}
