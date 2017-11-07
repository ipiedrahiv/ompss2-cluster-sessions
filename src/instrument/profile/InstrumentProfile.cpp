/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.
	
	Copyright (C) 2015-2017 Barcelona Supercomputing Center (BSC)
*/

#if HAVE_CONFIG_H
#include "config.h"
#endif

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200112L
#endif

#include "InstrumentProfile.hpp"
#include "InstrumentThreadLocalData.hpp"

#include "lowlevel/FatalErrorHandler.hpp"

#include <instrument/support/InstrumentThreadLocalDataSupport.hpp>
#include <instrument/support/InstrumentThreadLocalDataSupportImplementation.hpp>
#include <instrument/support/backtrace/BacktraceWalker.hpp>
#include <instrument/support/sampling/SigProf.hpp>

#include <atomic>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <sstream>
#include <string>


#include <cxxabi.h>
#if HAVE_LIBDW
#include <dwarf.h>
#include <elfutils/libdw.h>
#include <elfutils/libdwfl.h>
#endif


using namespace Instrument;


Instrument::Profile Instrument::Profile::_singleton;


void Instrument::Profile::signalHandler(Sampling::ThreadLocalData &samplingThreadLocal)
{
	ThreadLocalData &threadLocal = (ThreadLocalData &) samplingThreadLocal;
	
	long depth = _singleton._profilingBacktraceDepth;
	long bufferSize = _singleton._profilingBufferSize;
	
	if (threadLocal._nextBufferPosition + depth > (bufferSize + 2)) {
		int rc = posix_memalign((void **) &threadLocal._currentBuffer, 128, sizeof(address_t) * bufferSize);
		FatalErrorHandler::handle(rc, " allocating a buffer of ", sizeof(address_t) * bufferSize, " bytes for profiling");
		
		threadLocal._nextBufferPosition = 0;
		
		_singleton._bufferListSpinLock.lock();
		_singleton._bufferList.push_back(threadLocal._currentBuffer);
		_singleton._bufferListSpinLock.unlock();
	}
	
	{
		auto it = BacktraceWalker::begin();
		
		// Skip the signal handler frame and the signal frame
		++it; ++it;
		
		for (int currentFrame = 0; (currentFrame < depth) && (it != BacktraceWalker::end()); currentFrame++) {
			threadLocal._currentBuffer[threadLocal._nextBufferPosition] = *it;
			threadLocal._nextBufferPosition++;
			++it;
		}
	}
	
	// End of backtrace mark
	threadLocal._currentBuffer[threadLocal._nextBufferPosition] = 0;
	threadLocal._nextBufferPosition++;
	
	// We keep always an end mark in the buffer and add it to the list of buffers.
	// This way we do not need to perform any kind of cleanup for the threads
	threadLocal._currentBuffer[threadLocal._nextBufferPosition] = 0; // The end mark
}


thread_id_t Instrument::Profile::doCreatedThread()
{
	ThreadLocalData &threadLocal = getThreadLocalData();
	
	int rc = posix_memalign((void **) &threadLocal._currentBuffer, 128, sizeof(address_t) * _profilingBufferSize);
	FatalErrorHandler::handle(rc, " allocating a buffer of ", sizeof(address_t) * _profilingBufferSize, " bytes for profiling");
	
	threadLocal._nextBufferPosition = 0;
	
	// We keep always an end mark in the buffer and add it to the list of buffers.
	// This way we do not need to perform any kind of cleanup for the threads
	
	threadLocal._currentBuffer[threadLocal._nextBufferPosition] = 0; // End of backtrace
	threadLocal._currentBuffer[threadLocal._nextBufferPosition+1] = 0; // End of buffer
	
	_bufferListSpinLock.lock();
	_bufferList.push_back(threadLocal._currentBuffer);
	_bufferListSpinLock.unlock();
	
	Sampling::SigProf::setUpThread(threadLocal);
	
	// We call the signal handler once since the first call to backtrace allocates memory.
	// If the signal is delivered within a memory allocation, the thread can deadlock.
	Sampling::SigProf::forceHandler();
	
	// Remove the sample
	threadLocal._nextBufferPosition = 0;
	threadLocal._currentBuffer[0] = 0; // End of backtrace
	threadLocal._currentBuffer[1] = 0; // End of buffer
	
	Sampling::SigProf::enableThread(threadLocal);
	
	return thread_id_t();
}


inline std::string Instrument::Profile::demangleSymbol(std::string const &symbol)
{
	std::string result;
	
	int demangleStatus = 0;
	char *demangledName = abi::__cxa_demangle(symbol.c_str(), nullptr, 0, &demangleStatus);
	
	if ((demangledName != nullptr) && (demangleStatus == 0)) {
		result = demangledName;
	} else {
		result = symbol;
	}
	
	if (demangledName != nullptr) {
		free(demangledName);
	}
	
	return result;
}


#if HAVE_LIBDW
inline std::string Instrument::Profile::getDebugInformationEntryName(Dwarf_Die *debugInformationEntry)
{
	assert(debugInformationEntry != nullptr);
	
	Dwarf_Attribute linkageNameAttribute;
	
	// For some reason MIPS names have their own attribute type
	Dwarf_Attribute *attr = dwarf_attr_integrate(debugInformationEntry, DW_AT_MIPS_linkage_name, &linkageNameAttribute);
	
	if (attr == nullptr) {
		attr = dwarf_attr_integrate(debugInformationEntry, DW_AT_linkage_name, &linkageNameAttribute);
	}
	
	// This is useful for lambdas. In that case we get "operator()"
	if (attr == nullptr) {
		attr = dwarf_attr_integrate(debugInformationEntry, DW_AT_name, &linkageNameAttribute);
	}
	
	assert(attr == &linkageNameAttribute);
	
	char const *linkageName = dwarf_formstring(&linkageNameAttribute);
	
	if (linkageName == nullptr) {
		linkageName = dwarf_diename(debugInformationEntry);
	}
	
	if (linkageName == nullptr) {
		linkageName = "";
	}
	
	return demangleSymbol(linkageName);
}


inline std::string Instrument::Profile::sourceToString(char const *source, int line, int column)
{
	std::ostringstream oss;
	if (source != nullptr) {
		oss << source;
		if (line > 0) {
			oss << ":" << line;
			if (column > 0) {
				oss << ":" << column;
			}
		}
		
		return oss.str();
	}
	
	return std::string();
}


inline void Instrument::Profile::addInfoStep(AddrInfo &addrInfo, std::string function, std::string sourceLine)
{
	if (!function.empty() || !sourceLine.empty()) {
		AddrInfoStep addrInfoStep;
		
		if (!function.empty()) {
			auto functionIt = _sourceFunction2id.find(function);
			if (functionIt != _sourceFunction2id.end()) {
				addrInfoStep._functionId = functionIt->second;
			} else {
				addrInfoStep._functionId = _nextSourceFunctionId;
				_sourceFunction2id[function] = _nextSourceFunctionId;
				_id2sourceFunction[_nextSourceFunctionId] = function;
				_nextSourceFunctionId++;
			}
		}
		
		if (!sourceLine.empty()) {
			auto sourceLineIt = _sourceLine2id.find(sourceLine);
			if (sourceLineIt != _sourceLine2id.end()) {
				addrInfoStep._sourceLineId = sourceLineIt->second;
			} else {
				addrInfoStep._sourceLineId = _nextSourceLineId;
				_sourceLine2id[sourceLine] = _nextSourceLineId;
				_id2sourceLine[_nextSourceLineId] = sourceLine;
				_nextSourceLineId++;
			}
		}
		
		addrInfo.push_back(addrInfoStep);
	}
}
#endif


inline Instrument::Profile::AddrInfo const &Instrument::Profile::resolveAddress(Instrument::address_t address)
{
	{
		auto it = _addr2Cache.find(address);
		if (it != _addr2Cache.end()) {
			return it->second;
		}
	}
	
#if HAVE_LIBDW
	if (_dwfl == nullptr) {
		return _unknownAddrInfo;
	}
	
	Dwarf_Addr dwflAddress = (Dwarf_Addr) address;
	
	Dwfl_Module *module = dwfl_addrmodule(_dwfl, dwflAddress);
	if (module == nullptr) {
		return _unknownAddrInfo;
	}
	
	Dwarf_Addr addressBias = 0;
	Dwarf_Die *compilationUnitDebugInformationEntry = dwfl_module_addrdie(module, dwflAddress, &addressBias);
	
	assert(compilationUnitDebugInformationEntry != nullptr);
	Dwarf_Die *scopeDebugInformationEntries = nullptr;
	int scopeEntryCount = dwarf_getscopes(compilationUnitDebugInformationEntry, dwflAddress - addressBias, &scopeDebugInformationEntries);
	if (scopeEntryCount <= 0) {
		return _unknownAddrInfo;
	}
	
	// Get the name of the function
	std::string function;
	for (int scopeEntryIndex = 0; scopeEntryIndex < scopeEntryCount; scopeEntryIndex++) {
		Dwarf_Die *scopeEntry = &scopeDebugInformationEntries[scopeEntryIndex];
		int dwarfTag = dwarf_tag(scopeEntry);
		
		if (
			(dwarfTag == DW_TAG_subprogram)
			|| (dwarfTag == DW_TAG_inlined_subroutine)
			|| (dwarfTag == DW_TAG_entry_point)
		) {
			function = getDebugInformationEntryName(scopeEntry);
			break;
		}
	}
	
	// Get the source code location
	std::string sourceLine;
	{
		Dwfl_Line *dwarfLine = dwfl_module_getsrc (module, dwflAddress);
		
		Dwarf_Addr dwarfAddress = dwflAddress;
		int line = 0;
		int column = 0;
		const char *source = dwfl_lineinfo(dwarfLine, &dwarfAddress, &line, &column, nullptr, nullptr);
		
		sourceLine = sourceToString(source, line, column);
	}
	
	// Create and add the AddrInfoStep of the function
	AddrInfo addrInfo;
	addInfoStep(addrInfo, function, sourceLine);
	
	Dwarf_Off scopeOffset = dwarf_dieoffset(&scopeDebugInformationEntries[0]);
	Dwarf_Addr moduleBias = 0;
	Dwarf *moduleDwarf = dwfl_module_getdwarf(module, &moduleBias);
	Dwarf_Die addressDebugInformationEntry;
	dwarf_offdie(moduleDwarf, scopeOffset, &addressDebugInformationEntry);
	free(scopeDebugInformationEntries);
	
	scopeDebugInformationEntries = nullptr;
	scopeEntryCount = dwarf_getscopes_die(&addressDebugInformationEntry, &scopeDebugInformationEntries);
	
	if (scopeEntryCount > 1) {
		Dwarf_Die compilationUnit;
		Dwarf_Die *cu = dwarf_diecu(&scopeDebugInformationEntries[0], &compilationUnit, nullptr, nullptr);
		
		Dwarf_Files *sourceFiles = nullptr;
		
		int rc = -1;
		if (cu != nullptr) {
			assert(cu == &compilationUnit);
			
			// Load the source file information of the compilation unit if not already loaded
			rc = dwarf_getsrcfiles(&compilationUnit, &sourceFiles, nullptr);
		}
		
		if (rc == 0) {
			for (int scopeEntryIndex = 0; scopeEntryIndex < scopeEntryCount-1; scopeEntryIndex++) {
				Dwarf_Die *scopeEntry = &scopeDebugInformationEntries[scopeEntryIndex];
				{
					int dwarfTag = dwarf_tag(scopeEntry);
					
					if (dwarfTag != DW_TAG_inlined_subroutine) {
						continue;
					}
				}
				
				function.clear();
				sourceLine.clear();
				
				// Look up the function name
				for (int parentEntryIndex = scopeEntryIndex + 1; parentEntryIndex < scopeEntryCount; parentEntryIndex++) {
					Dwarf_Die *parentEntry = &scopeDebugInformationEntries[parentEntryIndex];
					int dwarfTag = dwarf_tag(parentEntry);
					
					if (
						(dwarfTag == DW_TAG_subprogram)
						|| (dwarfTag == DW_TAG_inlined_subroutine)
						|| (dwarfTag == DW_TAG_entry_point)
					) {
						function = getDebugInformationEntryName(parentEntry);
						break;
					}
				}
				
				// Get the source code location
				{
					int line = 0;
					int column = 0;
					const char *source = nullptr;
					
					Dwarf_Word attributeValue;
					Dwarf_Attribute attribute;
					
					// Get source file
					Dwarf_Attribute *filledAttribute = dwarf_attr(scopeEntry, DW_AT_call_file, &attribute);
					if (filledAttribute != nullptr) {
						assert(filledAttribute == &attribute);
						rc = dwarf_formudata(&attribute, &attributeValue);
						if (rc == 0) {
							source = dwarf_filesrc(sourceFiles, attributeValue, nullptr, nullptr);
						}
					}
					
					// Get line number
					filledAttribute = dwarf_attr(scopeEntry, DW_AT_call_line, &attribute);
					if (filledAttribute != nullptr) {
						assert(filledAttribute == &attribute);
						rc = dwarf_formudata(&attribute, &attributeValue);
						if (rc == 0) {
							line = attributeValue;
						}
					}
					
					// Get column number
					filledAttribute = dwarf_attr(scopeEntry, DW_AT_call_column, &attribute);
					if (filledAttribute != nullptr) {
						assert(filledAttribute == &attribute);
						rc = dwarf_formudata(&attribute, &attributeValue);
						if (rc == 0) {
							column = attributeValue;
						}
					}
					
					sourceLine = sourceToString(source, line, column);
				}
				
				addInfoStep(addrInfo, function, sourceLine);
			}
		}
		
		free(scopeDebugInformationEntries);
	}
	
	if (addrInfo.empty()) {
		return _unknownAddrInfo;
	} else {
		_addr2Cache[address] = std::move(addrInfo);
		return _addr2Cache[address];
	}
	
#else
	auto it = _executableMemoryMap.upper_bound(address);
	if (it == _executableMemoryMap.begin()) {
		// The address cannot be resolved
		return _unknownAddrInfo;
	}
	it--;
	
	MemoryMapSegment const &memoryMapSegment = it->second;
	
	if (memoryMapSegment._filename.empty()) {
		return _unknownAddrInfo;
	}
	
	
	AddrInfo addrInfo;
	
	size_t relativeAddress = (size_t)address - (size_t)it->first;
	
	std::ostringstream addr2lineCommandLine;
	addr2lineCommandLine << "addr2line -i -f -C -e " << memoryMapSegment._filename << " " << std::hex << relativeAddress;
	
	FILE *addr2lineOutput = popen(addr2lineCommandLine.str().c_str(), "r");
	if (addr2lineOutput == NULL) {
		perror("Error executing addr2line");
		exit(1);
	}
	
	char buffer[8192];
	buffer[8191] = 0;
	size_t length = fread(buffer, 1, 8191, addr2lineOutput);
	std::string cpp_buffer(buffer, length);
	pclose(addr2lineOutput);
	
	std::istringstream output(cpp_buffer);
	std::string function;
	std::string sourceLine;
	std::getline(output, function);
	std::getline(output, sourceLine);
	
	while (!output.eof()) {
		AddrInfoStep addInfoStep;
		
		if ((function != "??") && (sourceLine != "??:0") && (sourceLine != "??:?")) {
			auto functionIt = _sourceFunction2id.find(function);
			if (functionIt != _sourceFunction2id.end()) {
				addInfoStep._functionId = functionIt->second;
			} else {
				addInfoStep._functionId = _nextSourceFunctionId;
				_sourceFunction2id[function] = _nextSourceFunctionId;
				_id2sourceFunction[_nextSourceFunctionId] = function;
				_nextSourceFunctionId++;
			}
			
			auto sourceLineIt = _sourceLine2id.find(sourceLine);
			if (sourceLineIt != _sourceLine2id.end()) {
				addInfoStep._sourceLineId = sourceLineIt->second;
			} else {
				addInfoStep._sourceLineId = _nextSourceLineId;
				_sourceLine2id[sourceLine] = _nextSourceLineId;
				_id2sourceLine[_nextSourceLineId] = sourceLine;
				_nextSourceLineId++;
			}
			
			addrInfo.push_back(addInfoStep);
		}
		
		std::getline(output, function);
		std::getline(output, sourceLine);
	}
	
	_addr2Cache[address] = std::move(addrInfo);
	
	return _addr2Cache[address];
#endif
}


void Instrument::Profile::buildExecutableMemoryMap(pid_t pid)
{
#if HAVE_LIBDW
	static char *debugInfoPath = nullptr;
	
	static Dwfl_Callbacks dwflCallbacks = {
		dwfl_linux_proc_find_elf,
		dwfl_standard_find_debuginfo,
		nullptr,
		&debugInfoPath,
	};
	
	_dwfl = dwfl_begin(&dwflCallbacks);
	if (_dwfl == nullptr) {
		int error = dwfl_errno();
		std::cerr << "Warning: cannot get the memory map of the process: " << dwfl_errmsg(error) << std::endl;
		return;
	}
	
	int rc = dwfl_linux_proc_report(_dwfl, pid);
	if (rc != 0) {
		std::cerr << "Warning: cannot get the memory map of the process." << std::endl;
		return;
	}
#else
	std::string mapsFilename;
	{
		std::ostringstream oss;
		oss << "/proc/" << pid << "/maps";
		mapsFilename = oss.str();
	}
	std::ifstream mapsFile(mapsFilename.c_str());
	
	if (!mapsFile.is_open()) {
		std::cerr << "Warning: cannot get the memory map of the process from '" << mapsFilename << "'" << std::endl;
		return;
	}
	
	std::istringstream splitter;
	std::string field;
	std::istringstream hexDecoder;
	hexDecoder.setf(std::ios::hex, std::ios::basefield);
	while (!mapsFile.eof() && !mapsFile.bad()) {
		std::string line;
		std::getline(mapsFile, line);
		
		if (mapsFile.eof()) {
			break;
		} else if (mapsFile.bad()) {
			std::cerr << "Warning: error getting the memory map of the process from '" << mapsFilename << "'" << std::endl;
			break;
		}
		
		splitter.clear();
		splitter.str(line);
		
		// Memory start address
		size_t baseAddress;
		std::getline(splitter, field, '-');
		hexDecoder.clear();
		hexDecoder.str(field);
		hexDecoder >> baseAddress;
		
		MemoryMapSegment &memoryMapSegment = _executableMemoryMap[(address_t) baseAddress];
		
		// Memory end address + 1
		std::getline(splitter, field, ' ');
		hexDecoder.clear();
		hexDecoder.str(field);
		hexDecoder >> memoryMapSegment._length;
		memoryMapSegment._length -= baseAddress;
		
		// Permissions
		std::getline(splitter, field, ' ');
		
		// Offset
		std::getline(splitter, field, ' ');
		hexDecoder.clear();
		hexDecoder.str(field);
		hexDecoder >> memoryMapSegment._offset;
		
		// Device
		std::getline(splitter, field, ' ');
		
		// Inode
		long inode;
		splitter >> inode;
		
		// Path (if any)
		std::string path;
		std::getline(splitter, path);
		{
			size_t beginningOfPath = path.find_first_not_of(' ');
			if (beginningOfPath != std::string::npos) {
				path = path.substr(beginningOfPath);
				if (!path.empty() && (path[0] != '[')) {
					memoryMapSegment._filename = std::move(path);
				}
			}
		}
	}
	
	mapsFile.close();
#endif
}


void Instrument::Profile::doShutdown()
{
	// After this, on the next profiling signal, the corresponding timer gets disarmed
	Sampling::SigProf::disable();
	std::atomic_thread_fence(std::memory_order_seq_cst);
	
	#if !defined(HAVE_BACKTRACE) && !defined(HAVE_LIBUNWIND)
	return;
	#endif
	
	
	buildExecutableMemoryMap(getpid());
	
	
	// Build frequency tables and resolve address information
	std::map<address_t, freq_t> address2Frequency;
	std::map<Backtrace, freq_t> backtrace2Frequency;
	std::map<SymbolicBacktrace, freq_t> symbolicBacktrace2Frequency;
	
	{
		Backtrace backtrace(_profilingBacktraceDepth);
		SymbolicBacktrace symbolicBacktrace(_profilingBacktraceDepth);
		backtrace.clear();
		symbolicBacktrace.clear();
		int frame = 0;
		
		_bufferListSpinLock.lock();
		for (address_t *buffer : _bufferList) {
			long position = 0;
			frame = 0;
			
			while (position < _profilingBufferSize) {
				address_t address = buffer[position];
				
				if (address == 0) {
					if (frame == 0) {
						// End of buffer
						break;
					} else {
	// 					// End of backtrace
	// 					assert(frame <= _profilingBacktraceDepth);
	// 					for (; frame < _profilingBacktraceDepth; frame++) {
	// 						backtrace[frame] = 0;
	// 						symbolicBacktrace[frame].clear();
	// 					}
						
						// Increment the frequency of the backtrace
						{
							auto it = backtrace2Frequency.find(backtrace);
							if (it == backtrace2Frequency.end()) {
								backtrace2Frequency[backtrace] = 1;
							} else {
								it->second++;
							}
						}
						{
							auto it = symbolicBacktrace2Frequency.find(symbolicBacktrace);
							if (it == symbolicBacktrace2Frequency.end()) {
								symbolicBacktrace2Frequency[symbolicBacktrace] = 1;
							} else {
								it->second++;
							}
						}
						
						frame = 0;
						position++;
						backtrace.clear();
						symbolicBacktrace.clear();
						continue;
					}
				}
				
				AddrInfo const &addrInfo = resolveAddress(address);
				for (auto addrInfoStep : addrInfo) {
					if (addrInfoStep._functionId != id_t()) {
						_id2sourceFunction[addrInfoStep._functionId]._frequency++;
					}
					if (addrInfoStep._sourceLineId != id_t()) {
						_id2sourceLine[addrInfoStep._sourceLineId]._frequency++;
					}
				}
				
				backtrace.push_back(address);
				symbolicBacktrace.push_back(addrInfo);
				frame++;
				
				{
					auto it = address2Frequency.find(address);
					if (it != address2Frequency.end()) {
						it->second++;
					} else {
						address2Frequency[address] = 1;
					}
				}
				
				position++;
			}
			free(buffer);
		}
		_bufferList.clear();
		_bufferListSpinLock.unlock();
	}
	
	
	std::map<freq_t, std::list<Backtrace>, std::greater<freq_t>> backtracesByFrequency;
	for (auto it : backtrace2Frequency) {
		backtracesByFrequency[it.second].push_back(it.first);
	}
	backtrace2Frequency.clear();
	
	std::map<freq_t, std::list<SymbolicBacktrace>, std::greater<freq_t>> symbolicBacktracesByFrequency;
	for (auto it : symbolicBacktrace2Frequency) {
		symbolicBacktracesByFrequency[it.second].push_back(it.first);
	}
	symbolicBacktrace2Frequency.clear();
	
	
	{
		std::ostringstream oss;
		oss << "backtrace-profile-by-address-" << getpid() << ".txt";
		
		std::ofstream backtraceProfile(oss.str().c_str());
		for (auto it : backtracesByFrequency) {
			std::list<Backtrace> const &backtraces = it.second;
			for (Backtrace const &backtrace : backtraces) {
				bool first = true;
				for (address_t address : backtrace) {
					if (address == 0) {
						break;
					}
					
					AddrInfo const &addrInfo = resolveAddress(address);
					for (auto addrInfoStep : addrInfo) {
						if (first) {
							// Frequency on the innermost function
							backtraceProfile << it.first;
							first = false;
						}
						
						if (addrInfoStep._functionId != id_t()) {
							backtraceProfile << "\t" << _id2sourceFunction[addrInfoStep._functionId]._name;
						} else {
							backtraceProfile << "\t??";
						}
						
						if (addrInfoStep._sourceLineId != id_t()) {
							backtraceProfile << "\t" << _id2sourceLine[addrInfoStep._sourceLineId]._name;
						} else {
							backtraceProfile << "\t??:??";
						}
						
						backtraceProfile << std::endl;
					}
				}
				
				if (!first) {
					backtraceProfile << std::endl;
				}
			}
		}
		backtraceProfile.close();
	}
	backtracesByFrequency.clear();
	
	
	{
		std::ostringstream oss;
		oss << "backtrace-profile-by-line-" << getpid() << ".txt";
		
		std::ofstream symbolicBacktraceProfile(oss.str().c_str());
		for (auto it : symbolicBacktracesByFrequency) {
			std::list<SymbolicBacktrace> const &symbolicBacktraces = it.second;
			for (SymbolicBacktrace const &symbolicBacktrace : symbolicBacktraces) {
				bool first = true;
				for (AddrInfo const &addrInfo : symbolicBacktrace) {
					for (auto addrInfoStep : addrInfo) {
						if (first) {
							// Frequency on the innermost function
							symbolicBacktraceProfile << it.first;
							first = false;
						}
						
						if (addrInfoStep._functionId != id_t()) {
							symbolicBacktraceProfile << "\t" << _id2sourceFunction[addrInfoStep._functionId]._name;
						} else {
							symbolicBacktraceProfile << "\t??";
						}
						
						if (addrInfoStep._sourceLineId != id_t()) {
							symbolicBacktraceProfile << "\t" << _id2sourceLine[addrInfoStep._sourceLineId]._name;
						} else {
							symbolicBacktraceProfile << "\t??:??";
						}
						
						symbolicBacktraceProfile << std::endl;
					}
				}
				
				if (!first) {
					symbolicBacktraceProfile << std::endl;
				}
			}
		}
		symbolicBacktraceProfile.close();
	}
	symbolicBacktracesByFrequency.clear();
	
	
	std::map<freq_t, std::list<address_t>, std::greater<freq_t>> addressesByFrequency;
	for (auto it : address2Frequency) {
		addressesByFrequency[it.second].push_back(it.first);
	}
	address2Frequency.clear();
	
	{
		std::ostringstream oss;
		oss << "inline-profile-" << getpid() << ".txt";
		
		std::ofstream inlineProfile(oss.str().c_str());
		for (auto it : addressesByFrequency) {
			std::list<address_t> const &addresses = it.second;
			for (address_t address : addresses) {
				AddrInfo const &addrInfo = resolveAddress(address);
				if (!addrInfo.empty()) {
					// Frequency on the innermost function
					inlineProfile << it.first;
				}
				for (auto addrInfoStep : addrInfo) {
					if (addrInfoStep._functionId != id_t()) {
						inlineProfile << "\t" << _id2sourceFunction[addrInfoStep._functionId]._name;
					} else {
						inlineProfile << "\t??";
					}
					
					if (addrInfoStep._sourceLineId != id_t()) {
						inlineProfile << "\t" << _id2sourceLine[addrInfoStep._sourceLineId]._name;
					} else {
						inlineProfile << "\t??:??";
					}
					
					inlineProfile << std::endl;
				}
			}
		}
		inlineProfile.close();
	}
	addressesByFrequency.clear();
	
	
	std::map<freq_t, std::list<id_t>, std::greater<freq_t>> functionsByFrequency;
	for (auto it : _id2sourceFunction) {
		functionsByFrequency[it.second. _frequency].push_back(it.first);
	}
	
	{
		std::ostringstream oss;
		oss << "function-profile-" << getpid() << ".txt";
		
		std::ofstream functionProfile(oss.str().c_str());
		for (auto it : functionsByFrequency) {
			std::list<id_t> const &functions = it.second;
			for (id_t functionId : functions) {
				NameAndFrequency const &function = _id2sourceFunction[functionId];
				functionProfile << function._frequency << "\t" << function._name << "\n";
			}
		}
		functionProfile.close();
	}
	
	std::map<freq_t, std::list<id_t>, std::greater<freq_t>> linesByFrequency;
	for (auto it : _id2sourceLine) {
		linesByFrequency[it.second. _frequency].push_back(it.first);
	}
	
	{
		std::ostringstream oss;
		oss << "line-profile-" << getpid() << ".txt";
		
		std::ofstream lineProfile(oss.str().c_str());
		for (auto it : linesByFrequency) {
			std::list<id_t> const &lines = it.second;
			for (id_t lineId : lines) {
				NameAndFrequency const &line = _id2sourceLine[lineId];
				lineProfile << line._frequency << "\t" << line._name << "\n";
			}
		}
		lineProfile.close();
	}
	
#if HAVE_LIBDW
	if (_dwfl != nullptr) {
		dwfl_end(_dwfl);
	}
#endif
}

