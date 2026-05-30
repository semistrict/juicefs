/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package smartmap serves Linux Smartmap requests for mounted JuiceFS VFS
// instances.
//
// The package lives under pkg/vfs so it can use the exported VFS API while
// keeping userfaultfd-specific state and protocol handling out of the core VFS
// package. It is an efficient replacement for naive mmap VM memory: clients
// still see one contiguous file mapping, but cloned JuiceFS files can share
// clean pages instead of duplicating bytes per mapping.
package smartmap
