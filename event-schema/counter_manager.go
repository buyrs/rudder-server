package event_schema

// schemaHash -> Key -> FrequencyCounter
var countersCache map[string]map[string]*FrequencyCounter

type CounterItem struct {
	Value     string
	Frequency float64
}

func init() {
	if countersCache == nil {
		countersCache = make(map[string]map[string]*FrequencyCounter)
	}
}

// populateFrequencyCountersBounded is responsible for capturing the frequency counters which
// are available in the db and store them in memory but in a bounded manner.
func populateFrequencyCountersBounded(schemaHash string, frequencyCounters []*FrequencyCounter, bound int) {
	frequencyCountersMap := make(map[string]*FrequencyCounter)
	count := 0

	for _, fc := range frequencyCounters {
		// If count exceeds for a particular schema hash, break
		// the loop
		if count >= bound {
			break
		}

		frequencyCountersMap[fc.Name] = NewPeristedFrequencyCounter(fc)
		count++
	}
	countersCache[schemaHash] = frequencyCountersMap
}

// populateFrequencyCounters is responsible for capturing the frequency counters
// for a particular schema version into memory for fast computation.
func populateFrequencyCounters(schemaHash string, frequencyCounters []*FrequencyCounter) {
	populateFrequencyCountersBounded(schemaHash, frequencyCounters, frequencyCounterLimit)
	return
}

func getAllFrequencyCounters(schemaHash string) []*FrequencyCounter {
	schemaVersionCounters, ok := countersCache[schemaHash]
	if !ok {
		return []*FrequencyCounter{}
	}

	frequencyCounters := make([]*FrequencyCounter, 0, len(schemaVersionCounters))

	for _, v := range schemaVersionCounters {
		frequencyCounters = append(frequencyCounters, v)
	}
	return frequencyCounters
}

func getFrequencyCounterBounded(schemaHash string, key string, bound int) *FrequencyCounter {

	schemaVersionCounters, ok := countersCache[schemaHash]
	if !ok {
		schemaVersionCounters = make(map[string]*FrequencyCounter)
		countersCache[schemaHash] = schemaVersionCounters
	}

	diff := bound - len(schemaVersionCounters)
	// bound reached, not allowed adding more values.
	if diff == 0 {
		// Just check and return value from the map
		// no need to add anything to it.
		return schemaVersionCounters[key]
	}

	// If we have exceeded the bound, we need to trim it
	// to the new bound. This way whatever we have stored in memory
	// gets purged which will be flushed back to the database on a schedule.
	if diff < 0 {

		toDelete := -1 * diff
		for k := range schemaVersionCounters {
			if toDelete > 0 {
				delete(schemaVersionCounters, k)
				toDelete--
			} else {
				break
			}
		}

		// Once the values are trimmed, simply return the lookup
		return schemaVersionCounters[key]
	}

	// Here we add a new frequency counter for schemaVersionCounter
	frequencyCounter, ok := schemaVersionCounters[key]
	if !ok {
		frequencyCounter = NewFrequencyCounter(key)
		schemaVersionCounters[key] = frequencyCounter
	}

	return frequencyCounter
}

func getFrequencyCounter(schemaHash string, key string) *FrequencyCounter {
	return getFrequencyCounterBounded(schemaHash, key, frequencyCounterLimit)
}

func getSchemaVersionCounters(schemaHash string) map[string][]*CounterItem {
	schemaVersionCounters := countersCache[schemaHash]
	counters := make(map[string][]*CounterItem)

	for key, fc := range schemaVersionCounters {

		entries := fc.ItemsAboveThreshold()
		counterItems := make([]*CounterItem, 0, len(entries))
		for _, entry := range entries {

			freq := entry.Frequency
			// Capping the freq to 1
			if freq > 1 {
				freq = 1.0
			}
			counterItems = append(counterItems, &CounterItem{Value: entry.Key, Frequency: freq})
		}

		if len(counterItems) > 0 {
			counters[key] = counterItems
		}
	}
	return counters
}
