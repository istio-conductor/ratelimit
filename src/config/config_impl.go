package config

import (
	"fmt"
	"strings"

	pb_struct "github.com/envoyproxy/go-control-plane/envoy/extensions/common/ratelimit/v3"
	pb "github.com/envoyproxy/go-control-plane/envoy/service/ratelimit/v3"
	stats "github.com/lyft/gostats"
	logger "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"gopkg.in/yaml.v2"
)

type yamlRateLimit struct {
	RequestsPerUnit uint32 `yaml:"requests_per_unit"`
	Unit            string
}

type yamlDescriptor struct {
	Key         string
	Value       string
	RateLimit   *yamlRateLimit `yaml:"rate_limit"`
	Descriptors []yamlDescriptor
}

type yamlRoot struct {
	Domain      string
	Descriptors []yamlDescriptor
}

type rateLimitConfigImpl struct {
	domains    map[string]*RateLimitDomain
	statsScope stats.Scope
}

var validKeys = map[string]bool{
	"domain":            true,
	"key":               true,
	"value":             true,
	"descriptors":       true,
	"rate_limit":        true,
	"unit":              true,
	"requests_per_unit": true,
}

// Create new rate limit stats for a config entry.
// @param statsScope supplies the owning scope.
// @param key supplies the fully resolved key name of the entry.
// @return new stats.
func newRateLimitStats(statsScope stats.Scope, key string) RateLimitStats {
	ret := RateLimitStats{}
	ret.TotalHits = statsScope.NewCounter(key + ".total_hits")
	ret.OverLimit = statsScope.NewCounter(key + ".over_limit")
	ret.NearLimit = statsScope.NewCounter(key + ".near_limit")
	ret.OverLimitWithLocalCache = statsScope.NewCounter(key + ".over_limit_with_local_cache")
	ret.WithinLimit = statsScope.NewCounter(key + ".within_limit")
	return ret
}

// Create a new rate limit config entry.
// @param requestsPerUnit supplies the requests per unit of time for the entry.
// @param unit supplies the unit of time for the entry.
// @param key supplies the fully resolved key name of the entry.
// @param scope supplies the owning scope.
// @return the new config entry.
func NewRateLimit(
	requestsPerUnit uint32, unit pb.RateLimitResponse_RateLimit_Unit, key string, scope stats.Scope) *RateLimit {

	return &RateLimit{FullKey: key, Stats: newRateLimitStats(scope, key), Limit: &pb.RateLimitResponse_RateLimit{RequestsPerUnit: requestsPerUnit, Unit: unit}}
}

// Dump an individual descriptor for debugging purposes.
func (this *RateLimitDescriptor) dump() string {
	ret := ""
	if this.Limit != nil {
		ret += fmt.Sprintf(
			"%s: unit=%s requests_per_unit=%d\n", this.Limit.FullKey,
			this.Limit.Limit.Unit.String(), this.Limit.Limit.RequestsPerUnit)
	}
	for _, descriptor := range this.Descriptors {
		ret += descriptor.dump()
	}
	return ret
}

// Create a new config error which includes the owning file.
// @param config supplies the config file that generated the error.
// @param err supplies the error string.
func newRateLimitConfigError(config RateLimitConfigToLoad, err string) RateLimitConfigError {
	return RateLimitConfigError(fmt.Sprintf("%s: %s", config.Name, err))
}

// Load a set of config descriptors from the YAML file and check the input.
// @param config supplies the config file that owns the descriptor.
// @param parentKey supplies the fully resolved key name that owns this config level.
// @param descriptors supplies the YAML descriptors to load.
// @param statsScope supplies the owning scope.
func (this *RateLimitDescriptor) loadDescriptors(
	config RateLimitConfigToLoad, parentKey string, descriptors []yamlDescriptor,
	statsScope stats.Scope) {

	for _, descriptorConfig := range descriptors {
		if descriptorConfig.Key == "" {
			panic(newRateLimitConfigError(config, "descriptor has empty key"))
		}

		// Value is optional, so the final key for the map is either the key only or key_value.
		finalKey := descriptorConfig.Key
		if descriptorConfig.Value != "" {
			finalKey += "_" + descriptorConfig.Value
		}

		newParentKey := parentKey + finalKey
		if _, present := this.Descriptors[finalKey]; present {
			panic(newRateLimitConfigError(
				config, fmt.Sprintf("duplicate descriptor composite key '%s'", newParentKey)))
		}

		var rateLimit *RateLimit = nil
		var rateLimitDebugString string = ""
		if descriptorConfig.RateLimit != nil {
			value, present :=
				pb.RateLimitResponse_RateLimit_Unit_value[strings.ToUpper(descriptorConfig.RateLimit.Unit)]
			if !present || value == int32(pb.RateLimitResponse_RateLimit_UNKNOWN) {
				panic(newRateLimitConfigError(
					config,
					fmt.Sprintf("invalid rate limit unit '%s'", descriptorConfig.RateLimit.Unit)))
			}

			rateLimit = NewRateLimit(
				descriptorConfig.RateLimit.RequestsPerUnit, pb.RateLimitResponse_RateLimit_Unit(value), newParentKey,
				statsScope)
			rateLimitDebugString = fmt.Sprintf(
				" ratelimit={requests_per_unit=%d, unit=%s}", rateLimit.Limit.RequestsPerUnit,
				rateLimit.Limit.Unit.String())
		}

		logger.Debugf(
			"loading descriptor: key=%s%s", newParentKey, rateLimitDebugString)
		newDescriptor := &RateLimitDescriptor{map[string]*RateLimitDescriptor{}, rateLimit}
		newDescriptor.loadDescriptors(
			config, newParentKey+".", descriptorConfig.Descriptors, statsScope)
		this.Descriptors[finalKey] = newDescriptor
	}
}

// Validate a YAML config file's keys.
// @param config specifies the file contents to load.
// @param any specifies the yaml file and a map.
func validateYamlKeys(config RateLimitConfigToLoad, config_map map[interface{}]interface{}) {
	for k, v := range config_map {
		if _, ok := k.(string); !ok {
			errorText := fmt.Sprintf("config error, key is not of type string: %v", k)
			logger.Debugf(errorText)
			panic(newRateLimitConfigError(config, errorText))
		}
		if _, ok := validKeys[k.(string)]; !ok {
			errorText := fmt.Sprintf("config error, unknown key '%s'", k)
			logger.Debugf(errorText)
			panic(newRateLimitConfigError(config, errorText))
		}
		switch v := v.(type) {
		case []interface{}:
			for _, e := range v {
				if _, ok := e.(map[interface{}]interface{}); !ok {
					errorText := fmt.Sprintf("config error, yaml file contains list of type other than map: %v", e)
					logger.Debugf(errorText)
					panic(newRateLimitConfigError(config, errorText))
				}
				element := e.(map[interface{}]interface{})
				validateYamlKeys(config, element)
			}
		case map[interface{}]interface{}:
			validateYamlKeys(config, v)
		// string is a leaf type in ratelimit config. No need to keep validating.
		case string:
		// int is a leaf type in ratelimit config. No need to keep validating.
		case int:
		// nil case is an incorrectly formed yaml. However, because this function's purpose is to validate
		// the yaml's keys we don't panic here.
		case nil:
		default:
			errorText := fmt.Sprintf("error checking config")
			logger.Debugf(errorText)
			panic(newRateLimitConfigError(config, errorText))
		}
	}
}

// Load a single YAML config file into the global config.
// @param config specifies the file contents to load.
func (this *rateLimitConfigImpl) loadConfig(config RateLimitConfigToLoad) {
	// validate keys in config with generic map
	any := map[interface{}]interface{}{}
	err := yaml.Unmarshal([]byte(config.FileBytes), &any)
	if err != nil {
		errorText := fmt.Sprintf("error loading config file: %s", err.Error())
		logger.Debugf(errorText)
		panic(newRateLimitConfigError(config, errorText))
	}
	validateYamlKeys(config, any)

	var root yamlRoot
	err = yaml.Unmarshal([]byte(config.FileBytes), &root)
	if err != nil {
		errorText := fmt.Sprintf("error loading config file: %s", err.Error())
		logger.Debugf(errorText)
		panic(newRateLimitConfigError(config, errorText))
	}

	if root.Domain == "" {
		panic(newRateLimitConfigError(config, "config file cannot have empty domain"))
	}

	if _, present := this.domains[root.Domain]; present {
		panic(newRateLimitConfigError(
			config, fmt.Sprintf("duplicate domain '%s' in config file", root.Domain)))
	}

	logger.Debugf("loading domain: %s", root.Domain)
	newDomain := &RateLimitDomain{RateLimitDescriptor{map[string]*RateLimitDescriptor{}, nil}}
	newDomain.loadDescriptors(config, root.Domain+".", root.Descriptors, this.statsScope)
	this.domains[root.Domain] = newDomain
}

func (this *rateLimitConfigImpl) descriptorToKey(descriptor *pb_struct.RateLimitDescriptor) string {
	rateLimitKey := ""
	for _, entry := range descriptor.Entries {
		if rateLimitKey != "" {
			rateLimitKey += "."
		}
		rateLimitKey += entry.Key
		if entry.Value != "" {
			rateLimitKey += "_" + entry.Value
		}
	}
	return rateLimitKey
}

func (this *rateLimitConfigImpl) Dump() string {
	ret := ""
	for _, domain := range this.domains {
		ret += domain.dump()
	}

	return ret
}

func (this *rateLimitConfigImpl) GetLimit(
	ctx context.Context, domain string, descriptor *pb_struct.RateLimitDescriptor) *RateLimit {

	logger.Debugf("starting get limit lookup")
	var rateLimit *RateLimit = nil
	value := this.domains[domain]
	if value == nil {
		logger.Debugf("unknown domain '%s'", domain)
		return rateLimit
	}

	if descriptor.GetLimit() != nil {
		rateLimitKey := domain + "." + this.descriptorToKey(descriptor)
		rateLimitOverrideUnit := pb.RateLimitResponse_RateLimit_Unit(descriptor.GetLimit().GetUnit())
		rateLimit = NewRateLimit(
			descriptor.GetLimit().GetRequestsPerUnit(),
			rateLimitOverrideUnit,
			rateLimitKey,
			this.statsScope)
		return rateLimit
	}

	descriptorsMap := value.Descriptors
	for i, entry := range descriptor.Entries {
		// First see if key_value is in the map. If that isn't in the map we look for just key
		// to check for a default value.
		finalKey := entry.Key + "_" + entry.Value
		logger.Debugf("looking up key: %s", finalKey)
		nextDescriptor := descriptorsMap[finalKey]
		if nextDescriptor == nil {
			finalKey = entry.Key
			logger.Debugf("looking up key: %s", finalKey)
			nextDescriptor = descriptorsMap[finalKey]
		}

		if nextDescriptor != nil && nextDescriptor.Limit != nil {
			logger.Debugf("found rate limit: %s", finalKey)
			if i == len(descriptor.Entries)-1 {
				rateLimit = nextDescriptor.Limit
			} else {
				logger.Debugf("request depth does not match config depth, there are more entries in the request's descriptor")
			}
		}

		if nextDescriptor != nil && len(nextDescriptor.Descriptors) > 0 {
			logger.Debugf("iterating to next level")
			descriptorsMap = nextDescriptor.Descriptors
		} else {
			break
		}
	}

	return rateLimit
}

func (this *rateLimitConfigImpl) GetDescriptor(domain string) *RateLimitDescriptor {
	if domain, ok := this.domains[domain]; !ok {
		return nil
	} else {
		return &domain.RateLimitDescriptor
	}
}

func (this *rateLimitConfigImpl) UpdateDescriptor(domain string, des *RateLimitDescriptor) {
	this.domains[domain] = &RateLimitDomain{RateLimitDescriptor: *des}
}

// Create rate limit config from a list of input YAML files.
// @param configs specifies a list of YAML files to load.
// @param stats supplies the stats scope to use for limit stats during runtime.
// @return a new config.
func NewRateLimitConfigImpl(
	configs []RateLimitConfigToLoad, statsScope stats.Scope) RateLimitConfig {

	ret := &rateLimitConfigImpl{map[string]*RateLimitDomain{}, statsScope}
	for _, config := range configs {
		ret.loadConfig(config)
	}

	return ret
}

// NewRateLimitConfigDummyScope create a config without specific scope
func NewRateLimitConfigDummyScope(configs []RateLimitConfigToLoad) RateLimitConfig {
	dummyStats := stats.NewStore(stats.NewNullSink(), false)
	ret := &rateLimitConfigImpl{map[string]*RateLimitDomain{}, dummyStats}
	for _, config := range configs {
		ret.loadConfig(config)
	}
	return ret
}

type rateLimitConfigLoaderImpl struct{}

func (this *rateLimitConfigLoaderImpl) Load(
	configs []RateLimitConfigToLoad, statsScope stats.Scope) RateLimitConfig {

	return NewRateLimitConfigImpl(configs, statsScope)
}

// @return a new default config loader implementation.
func NewRateLimitConfigLoaderImpl() RateLimitConfigLoader {
	return &rateLimitConfigLoaderImpl{}
}
