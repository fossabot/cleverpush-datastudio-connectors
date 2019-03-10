function isAdminUser() {
  return true;
}

/**
 * Builds the Community Connector config.
 * @return {Config} The Community Connector config.
 * @see https://developers.google.com/apps-script/reference/data-studio/config
 */
function getConfig() {
  var cc = DataStudioApp.createCommunityConnector();
  var config = cc.getConfig();

  config.newInfo()
      .setId('instructions')
      .setText('Select the desired CleverPush channel.');

  var channelSelect = config.newSelectSingle()
      .setId('channel')
      .setName('CleverPush channel');
  var channels = getChannels();
  for (var i = 0; i < channels.length; i++) {
    var channel = channels[i];
    channelSelect.addOption(config.newOptionBuilder().setLabel(channel.name).setValue(channel._id))
  }

  config.setDateRangeRequired(true);

  return config.build();
}

/**
 * Builds the Community Connector fields object.
 * @return {Fields} The Community Connector fields.
 * @see https://developers.google.com/apps-script/reference/data-studio/fields
 */
function getFields() {
  var cc = DataStudioApp.createCommunityConnector();
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;

  fields.newDimension()
      .setId('date')
      .setName('Date')
      .setType(types.YEAR_MONTH_DAY_HOUR);

  fields.newDimension()
      .setId('channel')
      .setName('Channel')
      .setType(types.TEXT);

  fields.newMetric()
      .setId('sent')
      .setName('Sent')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);

  fields.newMetric()
      .setId('delivered')
      .setName('Delivered')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);

  fields.newMetric()
      .setId('clicked')
      .setName('Clicked')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);

  fields.newMetric()
      .setId('subscriptions')
      .setName('Subscriptions')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);

  fields.newMetric()
      .setId('optIns')
      .setName('OptIns')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);

  fields.newMetric()
      .setId('optOuts')
      .setName('OptOuts')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);

  return fields;
}

/**
 * Builds the Community Connector schema.
 * @param {object} request The request.
 * @return {object} The schema.
 */
function getSchema(request) {
  var fields = getFields().build();
  return {'schema': fields};
}

/**
 * Constructs an object with values as rows.
 * @param {Fields} requestedFields The requested fields.
 * @param {object[]} response The response.
 * @param {string} packageName The package name.
 * @return {object} An object containing rows with values.
 */
function responseToRows(requestedFields, response, channelId) {
  // Transform parsed data and filter for requested fields
  return response.map(function(stat) {
    var row = [];
    requestedFields.asArray().forEach(function(field) {
      switch (field.getId()) {
        case 'date':
          return row.push(stat.date ? stat.date.substr(0, 13).replace('T', '').replace(/-/g, '') : '');
        case 'sent':
          return row.push(stat.sent);
        case 'delivered':
          return row.push(stat.delivered);
        case 'clicked':
          return row.push(stat.clicked);
        case 'subscriptions':
          return row.push(stat.subscriptions);
        case 'optIns':
          return row.push(stat.optIns);
        case 'optOuts':
          return row.push(stat.optOuts);
        case 'channel':
          return row.push(channelId);
        default:
          return row.push('');
      }
    });
    return {values: row};
  });
}

/**
 * Gets the data for the community connector
 * @param {object} request The request.
 * @return {object} The data.
 */
function getData(request) {
  var requestedFieldIds = request.fields.map(function(field) {
    return field.name;
  });
  var requestedFields = getFields().forIds(requestedFieldIds);

  // Fetch and parse data from API
  var url = [
    'https://api.cleverpush.com/channel/', request.configParams.channel , '/statistics?aggregate=false'
  ];
  var response = UrlFetchApp.fetch(url.join(''), {
   "method" : "get",
     "headers" : {
       "Authorization": PropertiesService.getUserProperties().getProperty('dscc.key')
     }
  });
  var parsedResponse = JSON.parse(response).stats;
  var rows = responseToRows(requestedFields, parsedResponse, request.configParams.channel);

  return {
    schema: requestedFields.build(),
    rows: rows
  };
}

/**
 * Returns the Auth Type of this connector.
 * @return {object} The Auth type.
 */
function getAuthType() {
  var cc = DataStudioApp.createCommunityConnector();
  return cc.newAuthTypeResponse()
    .setAuthType(cc.AuthType.KEY)
    .setHelpUrl('https://cleverpush.com/app/settings/api')
    .build();
}

/**
 * Resets the auth service.
 */
function resetAuth() {
  var userProperties = PropertiesService.getUserProperties();
  userProperties.deleteProperty('dscc.key');
}

/**
 * Returns true if the auth service has access.
 * @return {boolean} True if the auth service has access.
 */
function isAuthValid() {
  var userProperties = PropertiesService.getUserProperties();
  var key = userProperties.getProperty('dscc.key');
  // This assumes you have a validateKey function that can validate
  // if the key is valid.
  return validateKey(key);
}

function getChannels() {
  var response = UrlFetchApp.fetch('https://api.cleverpush.com/channels', {
   "method" : "get",
     "headers" : {
       "Authorization": PropertiesService.getUserProperties().getProperty('dscc.key')
     }
  });
  var data = JSON.parse(response.getContentText());
  return data.channels;
}

function validateKey(key) {
  var response = UrlFetchApp.fetch('https://api.cleverpush.com/channels', {
   "method" : "get",
     "headers" : {
       "Authorization": key
     }
  });
  return response.getResponseCode() === 200;
}

/**
 * Sets the credentials.
 * @param {Request} request The set credentials request.
 * @return {object} An object with an errorCode.
 */
function setCredentials(request) {
  var key = request.key;

  // Optional
  // Check if the provided key is valid through a call to your service.
  // You would have to have a `checkForValidKey` function defined for
  // this to work.
  var validKey = validateKey(key);
  if (!validKey) {
    return {
      errorCode: 'INVALID_CREDENTIALS'
    };
  }
  var userProperties = PropertiesService.getUserProperties();
  userProperties.setProperty('dscc.key', key);
  return {
    errorCode: 'NONE'
  };
}
