// Retrieve button and text display elements
const foliumButton = document.getElementById("btn-folium");
const mapboxButton = document.getElementById("btn-mapbox");
const turfButton = document.getElementById("btn-turf");
const foliumCountDisplay = document.getElementById("count-folium");
const mapboxCountDisplay = document.getElementById("count-mapbox");
const turfCountDisplay = document.getElementById("count-turf");


// Initialize count variables
let countFolium = 0;
let countMapbox = 0;
let countTurf = 0;

// Use event listeners to track button clicks
// Increment respective count variables and update corresponding display elements on webpage
foliumButton.addEventListener("click", function () {
  countFolium++;
  foliumCountDisplay.innerHTML = countFolium;
});

mapboxButton.addEventListener("click", function () {
  countMapbox++;
  mapboxCountDisplay.innerHTML = countMapbox;
});

turfButton.addEventListener("click", function () {
  countTurf++;
  turfCountDisplay.innerHTML = countTurf;
});
