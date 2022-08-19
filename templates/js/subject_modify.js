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
        headers: new Headers({
            Accept: "application/json",
            "Content-Type": "application/json",
        }),
        body: JSON.stringify(requestBody),
    }).then(response => response.json()
        .then(data => {
            if (response.ok) {
                navigateToSuccessPage()
            } else {
                showErrorMessage(data["details"])
            }
        })).catch(_ => {
        showErrorMessage('Something went wrong. Please contact your system administrator.')
    });
};

const navigateToSuccessPage = () => {
    window.location.href = `${window.location.href}/success`;
}
