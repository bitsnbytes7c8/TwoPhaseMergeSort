#include<iostream>
#include<cstdio>
#include<cstdlib>
#include<cmath>
#include<cstring>
#include<string>
#include<vector>
#include<map>
#include<stack>
#include<queue>
#include<algorithm>
#include<fstream>

using namespace std;

long long block_size = 4096;

vector<vector<string> > data;
vector<int> priority;
char *buffer;
long long record_size, file_size, records_in_block, residual_records, blocks_in_memory, no_of_records, no_of_files; 


struct Ascending : public binary_function<vector<pair<string,int> >, vector<pair<string, int> >, bool>
{
	bool operator()(const vector<pair<string, int> > &d1, const vector<pair<string, int> > &d2)
	{
		int i= 0;
		for(i=0; i<priority.size(); i++)
		{
			if(d1[priority[i]].first == d2[priority[i]].first)
				continue;
			return (d1[priority[i]].first > d2[priority[i]].first);
		}
		return (d1[priority[i-1]].first < d2[priority[i-1]].first);
	}
};

struct Descending : public binary_function<vector<pair<string,int> >, vector<pair<string, int> >, bool>
{
	bool operator()(const vector<pair<string, int> > &d1, const vector<pair<string, int> > &d2)
	{
		int i= 0;
		for(i=0; i<priority.size(); i++)
		{
			if(d1[priority[i]].first == d2[priority[i]].first)
				continue;
			return (d1[priority[i]].first < d2[priority[i]].first);
		}
		return (d1[priority[i-1]].first < d2[priority[i-1]].first);
	}
};

bool compare1(const vector<string> &d1, const vector<string> &d2)
{
	int i = 0;
	for(i=0; i<priority.size(); i++)
	{
		if(d1[priority[i]] == d2[priority[i]])
			continue;
		return (d1[priority[i]] < d2[priority[i]]);
	}
	return (d1[priority[i-1]] < d2[priority[i-1]]);
}

bool compare2(const vector<string> &d1, const vector<string> &d2)
{
	int i=0;
	for(i=0; i<priority.size(); i++)
	{
		if(d1[priority[i]] == d2[priority[i]])
			continue;
		return (d1[priority[i]] > d2[priority[i]]);
	}
	return (d1[priority[i-1]] > d2[priority[i-1]]);
}

void merge(char order)
{
	ofstream out;
	out.open("Sorted", ios::out);
	ifstream in[1000];
	//vector<vector<pair<string, int> > > dt[1000];
	priority_queue<vector<pair<string, int> >, vector<vector<pair<string, int> > >, Ascending> pq_asc;
	priority_queue<vector<pair<string, int> >, vector<vector<pair<string, int> > >, Descending> pq_desc;
	if(blocks_in_memory < no_of_files)
	{
		cout<<"Can't merge\n";
		return;
	}
	long long completed[1000] = {0};
	char fileout[100], ot[5];
	char line[1000], word[1000];
	long long last_size;
	long long size = 0;
	string wrd;
	for(int i=0; i<no_of_files; i++)
	{
		sprintf(ot, "%d", i);
		strcpy(fileout, "Chunk/");
		strcat(fileout, ot);
		in[i].open(fileout, ios::in);
		//cout<<i<<" "<<in[i].read(buffer, records_in_block*record_size)<<endl;
		//cout<<"Buffer\n"<<buffer<<endl;
		if(i < no_of_files-1)
		{
			in[i].read(buffer, record_size*records_in_block);
			buffer[record_size*records_in_block] = '\0';
		}
		else
		{
			in[i].seekg(0, ios::end);
			int size = in[i].tellg();
			in[i].seekg(0, ios::beg);
			last_size = size/record_size;
			if(size>record_size*records_in_block)
				size = record_size*records_in_block;
			in[i].read(buffer, size);
			buffer[size] = '\0';
			//cout<<"Buffer "<<strlen(buffer)<<endl;
		}
		for(int pos = 0; buffer[pos]!='\0'; pos++)
		{
			if(buffer[pos] == '\n' || buffer[pos] == '\r')
				continue;
			int count = 0;
			while(buffer[pos] != '\n' && buffer[pos] != '\r' && buffer[pos]!='\0')
				line[count++] = buffer[pos++];
			line[count] = '\0';
			//cout<<line<<endl;
			vector<pair<string, int> > temp;
			temp.clear();
			for(int k=0; line[k]!='\0'; )
			{
				if(line[k]!=' ')
				{
					count = 0;
					while(!(line[k] == ' ' && line[k+1] == ' ') && line[k]!='\0')
					{
						word[count++] = line[k++];
					}
					word[count] = '\0';
					wrd.assign(word);
					temp.push_back(make_pair(wrd, i));
				}
				else 
					k++;
			}
			if(order == 'a')
				pq_asc.push(temp);
			else
				pq_desc.push(temp);
		}
	}
	//cout<<"Start Merge\n";
	int flag[1000] = {0};
	int file_index, end, current;
	while(!pq_asc.empty() || !pq_desc.empty())
	{
		//cout<<pq_asc.size()<<" ";
		vector<pair<string, int> > temp;
		temp.clear();
		if(order == 'a')
		{
			temp = pq_asc.top();
			pq_asc.pop();
		}
		else
		{
			temp = pq_desc.top();
			pq_desc.pop();
		}
		//cout<<pq_asc.size()<<endl;
		for(int j = 0; j<temp.size(); j++)
		{
			out<<temp[j].first;
			if(j!=temp.size()-1)
				out<<"  ";
		}
		out<<endl;
		file_index = temp[0].second;
		//cout<<file_index<<endl;
		completed[file_index]++;
		if(completed[file_index] < records_in_block)
			continue;
		//cout<<temp.size()<<" "<<file_index<<endl;
		if(flag[file_index] == 1)
			continue;
		current = in[file_index].tellg();
		in[file_index].read(buffer, record_size*records_in_block);
		if(in[file_index].eof())
		{
			sprintf(ot, "%d", file_index);
			strcpy(fileout, "Chunk/");
			strcat(fileout, ot);
			in[file_index].close();
			in[file_index].open(fileout, ios::in);
			in[file_index].seekg(0, ios::end);
			end = in[file_index].tellg();
			//cout<<in[file_index].tellg()<<" "<<end<<" "<<fileout<<endl;
			size = end-current;
			if(size<record_size*records_in_block)
			{
				in[file_index].seekg(current);
				in[file_index].read(buffer, size);
				buffer[size] = '\0';
			}
			//cout<<"Buffer "<<strlen(buffer)<<endl;
			//cout<<"Completed "<<file_index<<endl;
			flag[file_index] = 1;
		//	continue;
		}
		completed[file_index] = 0;
		//cout<<"Taking more from "<<file_index<<endl;
		for(int pos = 0; buffer[pos]!='\0'; pos++)
		{
			if(buffer[pos] == '\n' || buffer[pos] == '\r')
				continue;
			int count = 0;
			while(buffer[pos] != '\n' && buffer[pos] != '\r' && buffer[pos]!='\0')
				line[count++] = buffer[pos++];
			line[count] = '\0';
			//cout<<line<<endl;
			vector<pair<string, int> > temp2;
			for(int k=0; line[k]!='\0'; )
			{
				if(line[k]!=' ')
				{
					count = 0;
					while(!(line[k] == ' ' && line[k+1] == ' ') && line[k]!='\0')
					{
						word[count++] = line[k++];
					}
					word[count] = '\0';
					wrd.assign(word);
					temp2.push_back(make_pair(wrd, file_index));
				}
				else 
					k++;
			}
			if(order == 'a')
				pq_asc.push(temp2);
			else
				pq_desc.push(temp2);
		}
	}
	return;

}

void divide_and_sort(long long mem, char *filename, char order)
{
	ifstream in;
	in.open(filename, ios::in);
	string line2;
	getline(in, line2);
	record_size = line2.length()+1;
	in.seekg(0, ios::end);
	file_size = in.tellg();
	in.seekg(0, ios::beg);
	records_in_block = block_size/record_size;
	residual_records = block_size%record_size;
	blocks_in_memory = mem/(records_in_block*record_size);
	//cout<<blocks_in_memory<<endl;
	no_of_records = file_size/record_size;
	no_of_files = (file_size/mem) ;
	//cout<<no_of_files<<endl<<record_size<<endl<<block_size<<endl<<blocks_in_memory<<endl<<records_in_block<<endl;

	//ofstream *out = new ofstream[no_of_files];
	ofstream out[1000];

	char fileout[10], ot[5];
	char word[1000];
	char line[100000];
	string wrd;
	int i;

	for(i=0; i<no_of_files; i++)
	{
		//cout<<i<<endl;
		sprintf(ot, "%d", i);
		strcpy(fileout, "Chunk/");
		strcat(fileout, ot);
		//cout<<fileout<<endl;
		out[i].open(fileout, ios::out);
		int blocks_read = 0;
		data.clear();
		//cout<<file_size-in.tellg()<<" "<<record_size*records_in_block<<endl;
		while(blocks_read < blocks_in_memory)
		{
			if(in.read(buffer, records_in_block*record_size) == 0)
				break;
			buffer[records_in_block*record_size] = '\0';
			blocks_read++;
			int count_records = 0, count_columns = 0;
			for(int pos = 0; buffer[pos]!='\0'; pos++)
			{
				if(buffer[pos] == '\n' || buffer[pos] == '\r')
					continue;
				int count = 0;
				while(buffer[pos] != '\n' && buffer[pos] != '\r' && buffer[pos]!='\0')
					line[count++] = buffer[pos++];
				line[count] = '\0';
				//cout<<line<<endl;
				count_columns = 0;
				vector<string> temp;
				for(int k=0; line[k]!='\0'; )
				{
					if(line[k]!=' ')
					{
						count = 0;
						while(!(line[k] == ' ' && line[k+1] == ' ') && line[k]!='\0')
						{
							word[count++] = line[k++];
						}
						word[count] = '\0';
						wrd.assign(word);
						temp.push_back(wrd);
					}
					else 
						k++;
				}
				data.push_back(temp);
			}
		}
		if(order == 'a')
			sort(data.begin(), data.end(), compare1);
		else
			sort(data.begin(), data.end(), compare2);
		for(int k=0; k<data.size(); k++)
		{
			for(int l=0; l<data[k].size(); l++)
			{
				out[i]<<data[k][l];
				if(l == data[k].size()-1)
					break;
				else
					out[i]<<"  ";
			}
			out[i]<<endl;
		}
	}
	long long left_over = file_size - in.tellg();
	sprintf(ot, "%d", i);
	strcpy(fileout, "Chunk/");
	strcat(fileout, ot);
	//cout<<fileout<<endl;
	out[i].open(fileout, ios::out);
	int blocks_read = 0;
	data.clear();
	no_of_files += 1;
	while(blocks_read < blocks_in_memory)
	{
		if(in.read(buffer, left_over) == 0)
			break;
		buffer[left_over] = '\0';
		blocks_read++;
		int count_records = 0, count_columns = 0;
		for(int pos = 0; buffer[pos]!='\0'; pos++)
		{
			if(buffer[pos] == '\n' || buffer[pos] == '\r')
				continue;
			int count = 0;
			while(buffer[pos] != '\n' && buffer[pos] != '\r' && buffer[pos]!='\0')
				line[count++] = buffer[pos++];
			line[count] = '\0';
			count_columns = 0;
			vector<string> temp;
			for(int k=0; line[k]!='\0'; )
			{
				if(line[k]!=' ')
				{
					count = 0;
					while(!(line[k] == ' ' && line[k+1] == ' ') && line[k]!='\0')
					{
						word[count++] = line[k++];
					}
					word[count] = '\0';
					wrd.assign(word);
					temp.push_back(wrd);
				}
				else 
					k++;
			}
			data.push_back(temp);
		}
	}
	if(order == 'a')
		sort(data.begin(), data.end(), compare1);
	else
		sort(data.begin(), data.end(), compare2);
	for(int k=0; k<data.size(); k++)
	{
		for(int l=0; l<data[k].size(); l++)
		{
			out[i]<<data[k][l];
			if(l == data[k].size()-1)
				break;
			else
				out[i]<<"  ";
		}
		out[i]<<endl;
	}

}





int main(int argc, char **argv)
{
	long long int mem = atoi(argv[2])*1024*1024;
	buffer = (char*)malloc(mem);
  char order = argv[3][0];
	char columns[5];
	for(int i=4; i<argc; i++)
	{
		int count = 0;
		for(int j=1; argv[i][j]!='\0'; j++)
		{
			columns[count++] = argv[i][j];
		}
		columns[count] = '\0';
		priority.push_back(atoi(columns));
	}
	divide_and_sort(mem, argv[1], order);
	//no_of_files = 11;
	merge(order);
	return 0;
}
