package main

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type interval struct{ first, last uint64 }
type gtidSet map[string][]interval

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func parseGTIDSet(raw string) (gtidSet, error) {
	set := make(gtidSet)
	if strings.TrimSpace(raw) == "" {
		return set, nil
	}
	for _, group := range strings.Split(raw, ",") {
		parts := strings.Split(strings.TrimSpace(group), ":")
		if len(parts) < 2 || !uuidPattern.MatchString(parts[0]) {
			return nil, fmt.Errorf("GTID 分组格式无效：%q", group)
		}
		uuid := strings.ToLower(parts[0])
		for _, item := range parts[1:] {
			bounds := strings.Split(strings.TrimSpace(item), "-")
			if len(bounds) > 2 {
				return nil, fmt.Errorf("GTID 区间格式无效：%q", item)
			}
			first, err := parseSequence(bounds[0])
			if err != nil {
				return nil, fmt.Errorf("GTID 区间 %q 格式无效：%w", item, err)
			}
			last := first
			if len(bounds) == 2 {
				last, err = parseSequence(bounds[1])
				if err != nil {
					return nil, fmt.Errorf("GTID 区间 %q 格式无效：%w", item, err)
				}
			}
			if last < first {
				return nil, fmt.Errorf("GTID 区间起始值大于结束值：%q", item)
			}
			set[uuid] = append(set[uuid], interval{first, last})
		}
	}
	set.normalize()
	return set, nil
}

func parseSequence(raw string) (uint64, error) {
	n, err := strconv.ParseUint(raw, 10, 63)
	if err != nil || n == 0 || n > math.MaxInt64 {
		return 0, fmt.Errorf("GTID 序号必须在 1 到 %d 之间", int64(math.MaxInt64))
	}
	return n, nil
}

func (set gtidSet) normalize() {
	for uuid, ranges := range set {
		sort.Slice(ranges, func(i, j int) bool { return ranges[i].first < ranges[j].first })
		merged := ranges[:0]
		for _, current := range ranges {
			if len(merged) != 0 && current.first <= merged[len(merged)-1].last+1 {
				if current.last > merged[len(merged)-1].last {
					merged[len(merged)-1].last = current.last
				}
				continue
			}
			merged = append(merged, current)
		}
		set[uuid] = merged
	}
}

func (set gtidSet) add(other gtidSet) {
	for uuid, ranges := range other {
		set[uuid] = append(set[uuid], ranges...)
	}
	set.normalize()
}

func (set gtidSet) subtract(other gtidSet) gtidSet {
	result := make(gtidSet)
	for uuid, ranges := range set {
		covered := other[uuid]
		index := 0
		for _, wanted := range ranges {
			cursor := wanted.first
			for index < len(covered) && covered[index].last < cursor {
				index++
			}
			for next := index; next < len(covered) && covered[next].first <= wanted.last; next++ {
				if covered[next].first > cursor {
					result[uuid] = append(result[uuid], interval{cursor, covered[next].first - 1})
				}
				if covered[next].last >= wanted.last {
					cursor = wanted.last + 1
					break
				}
				if covered[next].last >= cursor {
					cursor = covered[next].last + 1
				}
			}
			if cursor <= wanted.last {
				result[uuid] = append(result[uuid], interval{cursor, wanted.last})
			}
		}
	}
	return result
}

func (set gtidSet) empty() bool {
	for _, ranges := range set {
		if len(ranges) != 0 {
			return false
		}
	}
	return true
}

func (set gtidSet) String() string {
	uuids := make([]string, 0, len(set))
	for uuid, ranges := range set {
		if len(ranges) != 0 {
			uuids = append(uuids, uuid)
		}
	}
	sort.Strings(uuids)
	groups := make([]string, 0, len(uuids))
	for _, uuid := range uuids {
		parts := []string{uuid}
		for _, current := range set[uuid] {
			item := strconv.FormatUint(current.first, 10)
			if current.last != current.first {
				item += "-" + strconv.FormatUint(current.last, 10)
			}
			parts = append(parts, item)
		}
		groups = append(groups, strings.Join(parts, ":"))
	}
	return strings.Join(groups, ",\n")
}
