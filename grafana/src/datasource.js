'use strict';

System.register(['lodash'], function (_export, _context) {
  "use strict";

  var _, _createClass, GenericDatasource;

  function _classCallCheck(instance, Constructor) {
    if (!(instance instanceof Constructor)) {
      throw new TypeError("Cannot call a class as a function");
    }
  }

  return {
    setters: [function (_lodash) {
      _ = _lodash.default;
    }],
    execute: function () {
      _createClass = function () {
        function defineProperties(target, props) {
          for (var i = 0; i < props.length; i++) {
            var descriptor = props[i];
            descriptor.enumerable = descriptor.enumerable || false;
            descriptor.configurable = true;
            if ("value" in descriptor) descriptor.writable = true;
            Object.defineProperty(target, descriptor.key, descriptor);
          }
        }

        return function (Constructor, protoProps, staticProps) {
          if (protoProps) defineProperties(Constructor.prototype, protoProps);
          if (staticProps) defineProperties(Constructor, staticProps);
          return Constructor;
        };
      }();

      _export('GenericDatasource', GenericDatasource = function () {
        function GenericDatasource(instanceSettings, $q, backendSrv, templateSrv) {
          _classCallCheck(this, GenericDatasource);

          this.type = instanceSettings.type;
          this.url = instanceSettings.url;
          this.name = instanceSettings.name;
          this.q = $q;
          this.backendSrv = backendSrv;
          this.templateSrv = templateSrv;
        }

        _createClass(GenericDatasource, [{
          key: 'query',
          value: function query(options) {
            var query = this.buildQueryParameters(options);
            query.targets = query.targets.filter(function (t) {
              return !t.hide;
            });

            if (query.targets.length <= 0) {
              return this.q.when({ data: [] });
            }

            return this.backendSrv.datasourceRequest({
              url: this.url + '/query',
              data: query,
              method: 'POST',
              headers: { 'Content-Type': 'application/json' }
            });
          }
        }, {
          key: 'testDatasource',
          value: function testDatasource() {
            return this.backendSrv.datasourceRequest({
              url: this.url + '/hc',
              method: 'GET'
            }).then(function (response) {
              if (response.status === 200) {
                return { status: "success", message: "Data source is working", title: "Success" };
              }
            });
          }
        },
        {
            key: 'getAggregators',
            value: function getAggregators(options) {
              var target = typeof options === "string" ? options : options.target;
              var interpolated = {
                target: this.templateSrv.replace(target, null, 'regex')
              };

              return this.backendSrv.datasourceRequest({
                url: this.url + '/query/aggregators',
                data: interpolated,
                method: 'POST',
                headers: { 'Content-Type': 'application/json' }
              }).then(this.mapToTextValue);
            }
          },
          {
              key: 'getUnits',
              value: function getUnits(options) {
                var target = typeof options === "string" ? options : options.target;
                var interpolated = {
                  target: this.templateSrv.replace(target, null, 'regex')
                };

                return this.backendSrv.datasourceRequest({
                  url: this.url + '/query/units',
                  data: interpolated,
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' }
                }).then(this.mapToTextValue);
              }
            },
        {
          key: 'metricFindQuery',
          value: function metricFindQuery(options) {
            var target = typeof options === "string" ? options : options.target;
            var interpolated = {
              target: this.templateSrv.replace(target, null, 'regex')
            };

            return this.backendSrv.datasourceRequest({
              url: this.url + '/query/measurements',
              data: interpolated,
              method: 'POST',
              headers: { 'Content-Type': 'application/json' }
            }).then(this.mapToTextValue);
          }
        }, {
            key: 'conditionTypes',
            value: function conditionTypes(options) {

              return this.backendSrv.datasourceRequest({
                  url: this.url + '/query/ctypes',
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' }
                }).then(this.mapToTextValue);
            }
          }, {
            key: 'tagFindQuery',
            value: function metricFindQuery(options) {
              var target = typeof options === "string" ? options : options.target;
              var interpolated = {
                target: this.templateSrv.replace(target, null, 'regex')
              };

              return this.backendSrv.datasourceRequest({
                url: this.url + '/query/tags',
                data: interpolated,
                method: 'POST',
                headers: { 'Content-Type': 'application/json' }
              }).then(this.mapToTextValue);
            }
          }, {
              key: 'fieldOptionsQuery',
              value: function fieldOptionsQuery(options) {
                var target = typeof options === "string" ? options : options.target;
                var interpolated = {
                  target: this.templateSrv.replace(target, null, 'regex')
                };

                return this.backendSrv.datasourceRequest({
                  url: this.url + '/query/fields',
                  data: interpolated,
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' }
                }).then(this.mapToTextValue);
              }
            }, {
          key: 'mapToTextValue',
          value: function mapToTextValue(result) {
            return _.map(result.data, function (d, i) {
              if (d && d.text && d.value) {
                return { text: d.text, value: d.value };
              }
              return { text: d, value: i };
            });
          }
        }, {
          key: 'buildQueryParameters',
          value: function buildQueryParameters(options) {
            var _this = this;

            // remove placeholder targets
            options.targets = _.filter(options.targets, function (target) {
              return target.target !== 'select metric';
            });

            var targets = _.map(options.targets, function (target) {
            	var ts = target.aggregator.args[0].value;
            	if(target.aggregator.unit=='mins') {
            		ts = target.aggregator.args[0].value*60; 
            	}else if(target.aggregator.unit=='hours') {
            		ts = target.aggregator.args[0].value*3600; 
            	}else if(target.aggregator.unit=='days') {
            		ts = target.aggregator.args[0].value*3600*24; 
            	}else if(target.aggregator.unit=='weeks') {
            		ts = target.aggregator.args[0].value*3600*24*7; 
            	}else if(target.aggregator.unit=='months') {
            		ts = target.aggregator.args[0].value*3600*24*30; 
            	}else if(target.aggregator.unit=='months') {
            		ts = target.aggregator.args[0].value*3600*24*365; 
            	}
              var req = {
                target: _this.templateSrv.replace(target.target),
                filters: target.filters,
                aggregator: target.aggregator,
                correlate: target.correlate,
                field: target.field,
                refId: target.refId,
                hide: target.hide,
                type: target.type || 'timeserie'
              };
              
              return req;
            });

            options.targets = targets;

            return options;
          }
        }]);

        return GenericDatasource;
      }());

      _export('GenericDatasource', GenericDatasource);
    }
  };
});
// # sourceMappingURL=datasource.js.map
