let schema = {};
let step = 0;
let key_value_tags = {};
let key_tags = [];

handle_step();

function generate_schema() {
  if (isValidForm("create_form")) {
    handle_generation();
  } else {
    scrollToTop();
  }
}

function upload_schema() {
  if (isValidForm("validate_form")) {
    handle_upload();
  } else {
    scrollToTop();
  }
}

function handle_upload() {
  const ownerName = document.getElementById("owner_name").value;
  const ownerEmail = document.getElementById("owner_email").value;
  const domain = document.getElementById("domain_form_1").value;
  const title = document.getElementById("title_form_1").value;
  const sensitivity = document.getElementById("select_sensitivity_1").value;
  const updateBehaviour = document.getElementById("select_behaviour").value;
  const responseTextElementTitle = document.getElementById(
    "upload-response-title"
  );
  const responseTextElement = document.getElementById("upload-response");
  const spinner = document.getElementsByClassName("loading-spinner")[1];

  const owners = [{ email: ownerEmail, name: ownerName }];
  schema.metadata.owners = owners;
  schema.metadata.key_only_tags = key_tags;
  schema.metadata.key_value_tags = key_value_tags;
  schema.metadata.update_behaviour = updateBehaviour;
  schema.metadata.domain = domain;
  schema.metadata.dataset = title;
  schema.metadata.sensitivity = sensitivity;

  spinner.style.display = "block";

  fetch(`/schema`, {
    method: "POST",
    headers: new Headers({
      Accept: "application/json",
      "Content-Type": "application/json",
    }),
    body: JSON.stringify(schema),
  })
    .then((response) =>
      response.json().then((result) => {
        spinner.style.display = "none";

        if (response.ok) {
          responseTextElement.innerHTML = `Schema created: <br>${result["details"]}</br>`;
          responseTextElement.classList.add("response-msg--success");
          responseTextElement.classList.remove("response-msg--error");
        } else {
          const errorDetail = createErrorDetail(result);
          responseTextElementTitle.innerHTML = "Errors:";
          responseTextElementTitle.classList.add("response-msg--error");

          responseTextElement.innerHTML = errorDetail;
          responseTextElement.classList.add("response-msg--error");
          responseTextElement.classList.remove("response-msg--success");
        }
      })
    )
    .catch(() => {
      spinner.style.display = "none";
      responseTextElement.innerHTML =
        "Error: There was a problem uploading the schema";
      responseTextElement.classList.add("response-msg--error");
      responseTextElement.classList.remove("response-msg--success");
    });
}

function handle_generation() {
  const sensitivity = document.getElementById("select_sensitivity").value;
  const domain = document.getElementById("domain").value;
  const title = document.getElementById("title").value;
  const file = document.getElementById("file").files[0];
  const responseTextElementTitle = document.getElementById(
    "generate-response-title"
  );
  const responseTextElement = document.getElementById("generate-response");
  const spinner = document.getElementsByClassName("loading-spinner")[0];

  if (sensitivity && domain && title && file) {
    responseTextElement.innerHTML = "Uploading";
    spinner.style.display = "block";
    responseTextElement.classList.remove("response-msg--error");
    responseTextElement.classList.remove("response-msg--success");

    let data = new FormData();
    data.append("file", file);

    fetch(`${sensitivity}/${domain}/${title}/generate`, {
      method: "POST",
      body: data,
    })
      .then((response) =>
        response.json().then((result) => {
          spinner.style.display = "none";

          if (response.ok) {
            schema = result;
            step = 1;
            handle_step();
            populate_table(result);
          } else {
            const errorDetail = createErrorDetail(result);
            responseTextElementTitle.innerHTML = "Errors:";
            responseTextElementTitle.classList.add("response-msg--error");

            responseTextElement.innerHTML = errorDetail;
            responseTextElement.classList.add("response-msg--error");
            responseTextElement.classList.remove("response-msg--success");
          }
        })
      )
      .catch(() => {
        spinner.style.display = "none";
        responseTextElement.innerHTML =
          "Error: There was a problem generating the schema";
        responseTextElement.classList.add("response-msg--error");
        responseTextElement.classList.remove("response-msg--success");
      });
  }
}

function populate_table(schema) {
  const table = document.getElementById("schema_table");
  schema.columns.forEach((col) => {
    const { data_type, allow_null, name } = col;
    const row = table.insertRow();
    row.innerHTML = `
        <td>${name}</td>
        <td>
            <select class="form-select_dropdown_table" selected="${data_type}" data-name="${name}" data-type="data_type">
            <option ${data_type === "Int64" ? "selected" : ""}>Int64</option>
            <option ${
              data_type === "Float64" ? "selected" : ""
            }>Float64</option>
            <option ${data_type === "object" ? "selected" : ""}>object</option>
            <option ${data_type === "date" ? "selected" : ""}>date</option>
            <option ${
              data_type === "boolean" ? "selected" : ""
            }>boolean</option>
            </select>
        </td>
        <td>
            <select class="form-select_dropdown_table selected="${allow_null}" data-name="${name}" data-type="allow_null">
            <option ${allow_null === true ? "selected" : ""}>true</option>
            <option ${allow_null === false ? "selected" : ""}>false</option>
            </select>
        </td>
        <td>
            <input class="form-input_input_text" type='number' value='' data-name="${name}" data-type="partition_index" />
        </td>
    `;
  });

  const selects = document.getElementsByClassName("form-select_dropdown_table");
  for (let i = 0; i < selects.length; i++) {
    selects[i].addEventListener("change", onDataTypeChange);
  }

  const inputs = document.getElementsByClassName("form-input_input_text");
  for (let i = 0; i < inputs.length; i++) {
    inputs[i].addEventListener("change", onDataTypeChange);
  }
}

function updateSchema(columnName, value, updateType) {
  const index = schema.columns.findIndex((col) => col.name === columnName);
  if (updateType === "data_type") {
    schema.columns[index].data_type = value;
  } else if (updateType === "allow_null") {
    schema.columns[index].allow_null = value;
  } else if (updateType === "partition_index") {
    schema.columns[index].partition_index = value ? parseInt(value) : null;
  }
  console.log(schema);
}

function onDataTypeChange(elem) {
  const target = elem.target;
  const value = target.value;
  const name = target.dataset.name;
  const updateType = target.dataset.type;
  return updateSchema(name, value === "" ? null : value, updateType);
}

function renderKeyValueTags() {
  const tagArea = document.getElementById("key_value_tags");
  while (tagArea.firstChild.id !== "key_value_false_tag") {
    tagArea.removeChild(tagArea.firstChild);
  }

  Object.keys(key_value_tags).forEach((key) => {
    const value = key_value_tags[key];
    const tagRow = document.createElement("div");
    tagRow.className = "key-value-input--row";
    tagRow.innerHTML = `
      <input class="form-input_text" type="text" value="${key}" disabled>
      <input class="form-input_text" type="text" value="${value}" disabled>
      <label class="form-input_label btn btn--secondary" for="key_value_tag_remove_${key}">Remove</label>
      <input class="form-input_file remove-key-value-tag" type="button" id="key_value_tag_remove_${key}" data-key="${key}">
      `;
    tagArea.insertBefore(
      tagRow,
      document.getElementById("key_value_false_tag")
    );
  });

  const removes = document.getElementsByClassName("remove-key-value-tag");
  for (let i = 0; i < removes.length; i++) {
    removes[i].addEventListener("click", removeKeyValueTag);
  }
}

function renderKeyTags() {
  const tagArea = document.getElementById("key_tags");
  while (tagArea.firstChild.id !== "key_false_tag") {
    tagArea.removeChild(tagArea.firstChild);
  }

  key_tags.forEach((key, index) => {
    const tagRow = document.createElement("div");
    tagRow.className = "key-input--row";
    tagRow.innerHTML = `
      <input class="form-input_text" type="text" value="${key}" disabled>
      <label class="form-input_label btn btn--secondary" for="key_tag_remove_${key}">Remove</label>
      <input class="form-input_file remove_key_tag" type="button" id="key_tag_remove_${key}" data-index="${index}" >
    `;
    tagArea.insertBefore(tagRow, document.getElementById("key_false_tag"));
  });

  const removes = document.getElementsByClassName("remove_key_tag");
  for (let i = 0; i < removes.length; i++) {
    removes[i].addEventListener("click", removeKeyTag);
  }
}

function createKeyValueTag() {
  const keyElem = document.getElementById("key_value_false_tag_key");
  const valueElem = document.getElementById("key_value_false_tag_value");
  const key = keyElem.value;
  const value = valueElem.value;
  if (key && value) {
    key_value_tags[key] = value;
    renderKeyValueTags();
    keyElem.value = "";
    valueElem.value = "";
  }
}

function createKeyTag() {
  const keyElem = document.getElementById("key_false_tag_key");
  const key = keyElem.value;
  if (key) {
    key_tags.push(key);
    renderKeyTags();
    keyElem.value = "";
  }
}

function removeKeyValueTag(elem) {
  const target = elem.target;
  const key = target.dataset.key;
  delete key_value_tags[key];
  renderKeyValueTags();
}

function removeKeyTag(elem) {
  const target = elem.target;
  const index = target.dataset.index;
  key_tags.splice(index, 1);
  renderKeyTags();
}

function handle_step() {
  if (step === 0) {
    document.getElementById("form_step_0").style.visibility = "visible";
    document.getElementById("form_step_1").style.visibility = "hidden";

    document.getElementById("file").onchange = () => {
      document.getElementById("filename").textContent =
        document.getElementById("file").files[0].name;
    };
  } else if (step === 1) {
    document.getElementById("form_step_0").style.visibility = "hidden";
    document.getElementById("form_step_1").style.visibility = "visible";

    document.getElementById("domain_form_1").value = schema.metadata.domain;
    document.getElementById("title_form_1").value = schema.metadata.dataset;
    document.getElementById("select_sensitivity_1").value = schema.metadata.sensitivity;
    renderKeyValueTags();
    renderKeyTags();
  }
}
