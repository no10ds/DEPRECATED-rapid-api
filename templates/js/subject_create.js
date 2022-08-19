toggle_inputs("select_subject", "CLIENT", "email_input");

function create_subject() {
  const name = document.getElementById("name").value;
  const permissions = extract_selections();
  const subject_type = get_selected_value("select_subject");

  if (subject_type == "User") {
    const email = document.getElementById("email").value;
    create_user(name, email, permissions);
  } else if (subject_type == "Client") {
    create_client(name, permissions);
  } else {
    return null;
  }
}

function create_user(name, email, permissions) {
  const requestBody = {
    username: name,
    email: email,
    permissions: permissions.map((element) => element.value),
  };

  render_creation_success("user", requestBody);
}

function create_client(name, permissions) {
  const requestBody = {
    client_name: name,
    permissions: permissions.map((element) => element.value),
  };

  render_creation_success("client", requestBody);
}

function render_creation_success(type, requestBody) {
  fetch(`/${type}`, {
    method: "POST",
    headers: new Headers({
      Accept: "application/json",
      "Content-Type": "application/json",
    }),
    body: JSON.stringify(requestBody),
  }).then(response => response.json()
        .then(data => {
            if (response.ok) {
              load_success_html(data, type)
              hide_elements(["create_form", "submit_form"])
            } else {
                showErrorMessage(data["details"])
            }
        })).catch(_ => {
        showErrorMessage('Something went wrong. Please contact your system administrator.')
    });
};

function load_success_html(response_data, type) {

  const is_client = type == "client";
  const html = '<div class="form_body form_body--headed" id="success_form">' +
               '<h1 class="content_header">Success</h1>' +
               '<h2 class="content_header">You will only see this page once. Please make note of the details below.</h2>' +
               data_for_subject(is_client, response_data) +
               '</div>'
  document.getElementById("success").innerHTML = html;
}

function data_for_subject(is_client, response_data){
    if (is_client) {
        return '<h3>Client <span class="highlight">' + response_data.client_name + '</span> created</h3>' +
               '<h3>Id: <span class="highlight">' + response_data.client_id + '</span></h3>' +
               '<h3>Secret: <span class="highlight">' + response_data.client_secret + '</span></h3>';
    } else {
        return '<h3>User <span class="highlight">' + response_data.username + '</span> created</h3>' +
               '<h3>Id: <span class="highlight">' + response_data.user_id + '</span></h3>' +
               '<h3>Email: <span class="highlight">' + response_data.email+ '</span></h3>';
    }
}

function hide_elements(element_ids) {
  element_ids.forEach(element_id => document.getElementById(element_id).hidden = true);
}
