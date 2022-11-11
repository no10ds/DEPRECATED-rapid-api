var sessionTimer = function () {
  const LONG_TIMEOUT_PAGES = ["/upload", "/download/"];
  let time;
  window.onload = resetTimer;
  // DOM Events
  document.onmousemove = resetTimer;
  document.onmousedown = resetTimer;
  document.ontouchstart = resetTimer;
  document.onclick = resetTimer;
  document.onkeydown = resetTimer;

  function logout() {
    window.location.href = `${location.protocol}//${location.host}/logout`;
  }

  function resetTimer() {
    clearTimeout(time);
    time = setTimeout(logout, setTimeoutForPages());
  }

  function setTimeoutForPages() {
    //  300000ms == 5min
    let timeoutInMs = 300000 * 12;
    LONG_TIMEOUT_PAGES.forEach((page_uri) => {
      if (window.location.pathname.startsWith(page_uri)) {
        //  1800000ms == 30min
        timeoutInMs = 1800000;
      }
    });

    return timeoutInMs;
  }
};

window.onload = function () {
  sessionTimer();
};
