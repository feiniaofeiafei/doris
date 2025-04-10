// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_help_command", "query,help") {
    try {
        // Test the HELP command for a known topic
        checkNereidsExecute("HELP 'CREATE TABLE';")

        // Test the HELP command for an unknown topic
        checkNereidsExecute("HELP 'UNKNOWN_TOPIC';")

        // Test the HELP command for a keyword with multiple matches
        checkNereidsExecute("HELP 'ALTER';")
    } catch (Exception e) {
        // Log any exceptions that occur during testing
        log.error("Failed to execute HELP command", e)
        throw e
    }
}

