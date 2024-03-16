// Retrieve button and text display elements
const yesButton = document.getElementById("btn-yes");
const noButton = document.getElementById("btn-no");
const yesCountDisplay = document.getElementById("count-yes");
const noCountDisplay = document.getElementById("count-no");

// Initialize count variables
let countYes = 0;
let countNo = 0;

// Use event listeners to track button clicks
// Increment respective count variables and update corresponding display elements on webpage
yesButton.addEventListener("click", function () {
  countYes++;
  yesCountDisplay.innerHTML = countYes;
});

noButton.addEventListener("click", function () {
  countNo++;
  noCountDisplay.innerHTML = countNo;
});