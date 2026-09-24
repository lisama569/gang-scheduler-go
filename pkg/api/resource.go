package api

// Fits 判断 r 是否能放入容量 cap（不超过即算可行）。
func (r Resource) Fits(cap Resource) bool {
	return r.CPU <= cap.CPU && r.Memory <= cap.Memory
}

// Add 返回 r + other。
func (r Resource) Add(other Resource) Resource {
	return Resource{CPU: r.CPU + other.CPU, Memory: r.Memory + other.Memory}
}

// Sub 返回 r - other，调用方需保证不会出现负数语义。
func (r Resource) Sub(other Resource) Resource {
	return Resource{CPU: r.CPU - other.CPU, Memory: r.Memory - other.Memory}
}

// SumRequests 汇总若干任务的资源请求。
func SumRequests(tasks []TaskSpec) Resource {
	var total Resource
	for _, t := range tasks {
		total = total.Add(t.Resources)
	}
	return total
}
