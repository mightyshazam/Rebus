App.views.Endpoints = Backbone.View.extend({
  template: 
    '<div class="row">\
      <div class="span3">\
        <h2>Heading</h2>\
        <p>Donec id elit non mi porta gravida at eget metus. Fusce dapibus, tellus ac cursus commodo, tortor mauris condimentum nibh, ut fermentum massa justo sit amet risus. Etiam porta sem malesuada magna mollis euismod. Donec sed odio dui. </p>\
        <p><a class="btn" href="#">View details &raquo;</a></p>\
      </div>\
      <div class="span3">\
        <h2>Heading</h2>\
        <p>Donec id elit non mi porta gravida at eget metus. Fusce dapibus, tellus ac cursus commodo, tortor mauris condimentum nibh, ut fermentum massa justo sit amet risus. Etiam porta sem malesuada magna mollis euismod. Donec sed odio dui. </p>\
        <p><a class="btn" href="#">View details &raquo;</a></p>\
     </div>\
      <div class="span3">\
        <h2>Heading</h2>\
        <p>Donec sed odio dui. Cras justo odio, dapibus ac facilisis in, egestas eget quam. Vestibulum id ligula porta felis euismod semper. Fusce dapibus, tellus ac cursus commodo, tortor mauris condimentum nibh, ut fermentum massa justo sit amet risus.</p>\
        <p><a class="btn" href="#">View details &raquo;</a></p>\
      </div>\
    </div>\
    <hr>\
    <footer>\
      <p>&copy; Company 2012</p>\
    </footer>',

  initialize: function () {
    this.render();
  },

  render: function () {
    $(this.el).html(_.template(this.template));
    return this;
  }
});