function toggle_element(do_toggle, element_to_toggle_id) {
  element_to_toggle = document.getElementById(element_to_toggle_id);
  element_to_toggle.hidden = do_toggle;
  switch_enable_mode(do_toggle, element_to_toggle);
}

function switch_enable_mode(do_switch, parent_element) {
  [...parent_element.getElementsByTagName("input")].forEach(function (item) {
    item.disabled = do_switch;
  });
}

function toggle_inputs(element_id, value_to_check, element_to_toggle) {
  document.getElementById(element_id).onchange = () => {
    var name_to_bd =
      document.getElementById(element_id).value == value_to_check;
    toggle_element(name_to_bd, element_to_toggle);
  };
}

function toggle_check_boxes(radio_id) {
  if (radio_id.startsWith("READ_") || radio_id.startsWith("GLOBAL_READ")) {
    toggle_element(radio_id == "READ_ALL", "READ_PROTECTED");
  } else if (
    radio_id.startsWith("WRITE_") ||
    radio_id.startsWith("GLOBAL_WRITE")
  ) {
    toggle_element(radio_id == "WRITE_ALL", "WRITE_PROTECTED");
  }
}

function extract_selections() {
  const radios = document.querySelectorAll('input[type="radio"]');
  const checkboxes = document.querySelectorAll('input[type="checkbox"]');

  const checkedRadios = Array.from(radios).filter((radio) => radio.checked);
  const checkedBoxes = Array.from(checkboxes).filter(
    checkbox => !checkbox.disabled && checkbox.checked
  );

  const selections = checkedRadios.concat(checkedBoxes);

  const filteredSelections = selections.filter(
    (selection) => selection.value !== "NONE"
  );

  return filteredSelections;
}

function get_selected_value(element_id) {
  const element = document.getElementById(element_id);
  return element.options[element.selectedIndex].text;
}

const showErrorMessage = (message) => {
    const errorMessageContainer = document.getElementById('error-message')
    errorMessageContainer.innerText = message
    errorMessageContainer.hidden = false
}

const hideErrorMessage = () => {
    const errorMessageContainer = document.getElementById('error-message')
    errorMessageContainer.innerText = ''
    errorMessageContainer.hidden = true
}

const setupEventListeners = () => {
    // Set up inputs to clear error message on interaction
    const allInputs = document.querySelectorAll('input')
    allInputs.forEach(input => {
        input.addEventListener('click', hideErrorMessage)
        input.addEventListener('focus', hideErrorMessage)
    })
}
