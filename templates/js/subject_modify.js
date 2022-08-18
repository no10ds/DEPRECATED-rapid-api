toggle_element(document.getElementById("WRITE_ALL").checked, "WRITE_PROTECTED");
toggle_element(document.getElementById("READ_ALL").checked, "READ_PROTECTED");

const modify = (subjectId) => {
  const filteredSelections = extract_selections();

    const requestBody = {
        subject_id: subjectId,
        permissions: filteredSelections.map((element) => element.value),
    };

    fetch("/client/permissions", {
        method: "PUT",
        // This is needed to treat this call as a browser request
        headers: new Headers({
            Accept: "text/html,application/json",
            "Content-Type": "application/json",
        }),
        body: JSON.stringify(requestBody),
    }).then(response => response.json()
        .then(result => {
            if (response.ok) {
                navigateToSuccessPage()
            } else {
                showErrorMessage(result["details"])
            }
        })).catch(_ => {
        showErrorMessage('Something went wrong. Please contact your system administrator.')
    });
};

const navigateToSuccessPage = () => {
    window.location.href = `${window.location.href}/success`;
}

setupEventListeners()
