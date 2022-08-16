toggle_element(document.getElementById("WRITE_ALL").checked, "WRITE_PROTECTED");
toggle_element(document.getElementById("READ_ALL").checked, "READ_PROTECTED");

const modify = (subjectId) => {
    const radios = document.querySelectorAll('input[type="radio"]');
    const checkboxes = document.querySelectorAll('input[type="checkbox"]');

    const checkedRadios = Array.from(radios).filter((radio) => radio.checked);
    const checkedBoxes = Array.from(checkboxes).filter(
        (checkbox) => checkbox.checked
    );

    const selections = checkedRadios.concat(checkedBoxes);

    const filteredSelections = selections.filter(
        (selection) => selection.value !== "NONE"
    );

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

const showErrorMessage = (message) => {
    const errorMessageContainer = document.getElementById('error-message')
    errorMessageContainer.innerText = message
}

const hideErrorMessage = () => {
    const errorMessageContainer = document.getElementById('error-message')
    errorMessageContainer.innerText = ''
}

const setupEventListeners = () => {
    // Set up inputs to clear error message on interaction
    const allInputs = document.querySelectorAll('input')
    allInputs.forEach(input => {
        input.addEventListener('click', hideErrorMessage)
    })
}


setupEventListeners()
