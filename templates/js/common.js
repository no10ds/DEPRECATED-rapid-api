function toggle_element(do_toggle, element_to_toggle_id) {
    element_to_toggle = document.getElementById(element_to_toggle_id)
    element_to_toggle.hidden = do_toggle;
    switch_enable_mode(do_toggle, element_to_toggle);
}

function switch_enable_mode(do_switch, parent_element) {
    [...parent_element.getElementsByTagName('input')].forEach(function(item) {
      item.disabled = do_switch;
    });
}

function toggle_inputs(element_id, value_to_check, element_to_toggle) {
  document.getElementById(element_id).onchange = () => {
    var name_to_bd = document.getElementById(element_id).value == value_to_check
    toggle_element(name_to_bd, element_to_toggle);
  }
}


function toggle_check_boxes(radio_id, radio_name) {
  if(radio_name == "GLOBAL_READ") {
    toggle_element(radio_id == "READ_ALL", "READ_PROTECTED")
  }

  if(radio_name == "GLOBAL_WRITE") {
    toggle_element(radio_id == "WRITE_ALL", "WRITE_PROTECTED")
  }
}
